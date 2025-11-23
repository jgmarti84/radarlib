import datetime
import gc
import logging
from pathlib import Path
from typing import Dict

import pyart
from pyart.config import get_field_name

from radarlib import config
from radarlib.io.pyart.colmax import generate_colmax
from radarlib.io.pyart.filters import filter_fields_grc1
from radarlib.io.pyart.pyart_radar import estandarizar_campos_RMA, read_radar_netcdf
from radarlib.io.pyart.radar_png_plotter import FieldPlotConfig, RadarPlotConfig, plot_ppi_field, save_ppi_png
from radarlib.utils.fields_utils import get_lowest_nsweep
from radarlib.utils.names_utils import get_time_from_RMA_filename

# logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def process_volume(filename: str, vol_types: Dict, add_colmax: bool, logger_name=__name__) -> None:
    logger = logging.getLogger(logger_name)
    try:
        logger.debug(f"Procesando {filename}")

        # --- Load volume -----------------------------------------------------------------
        try:
            radar = read_radar_netcdf(filename)
            logger.debug(f"Volumen {filename} leído correctamente.")
        except Exception as e:
            logger.error(f"Leyendo el Volumen {filename}: {e}")
            # TODO: update database with error status

            # ctx.update_vols_in_db2(ctx.radar_name, date=filename.split('.')[0], integrity="FALLA",
            #                        error_type=f"Leyendo el Volumen: {e}", nprocess=1)
            gc.collect()
            return

        # --- Standardize fields ----------------------------------------------------------
        try:
            radar = estandarizar_campos_RMA(radar)
            logger.debug(f"Campos del volumen {filename} estandarizados correctamente.")
        except Exception as e:
            logger.error(f"Estandarizando Campos {filename}: {e}")
            # TODO: update database with error status
            # ctx.update_vols_in_db2(ctx.radar_name, date=filename.split('.')[0], integrity="FALLA",
            #                        error_type=f"Estandarizando_Campos: {e}", nprocess=1)
            gc.collect()
            return

        # --- Determine reflectivity fields (horizontal and vertical) ---
        fields = determine_reflectivity_fields(radar)
        hrefl_field = fields["hrefl_field"]
        hrefl_field_raw = fields["hrefl_field_raw"]
        vrefl_field = fields["vrefl_field"]
        vrefl_field_raw = fields["vrefl_field_raw"]

        # polarimetric and product field names: we use pyart.get_field_name
        rhv_field = get_field_name("cross_correlation_ratio")
        zdr_field = get_field_name("differential_reflectivity")
        cm_field = get_field_name("clutter_map")
        phidp_field = get_field_name("differential_phase")
        kdp_field = get_field_name("specific_differential_phase")
        vrad_field = get_field_name("velocity")
        wrad_field = get_field_name("spectrum_width")
        colmax_field = get_field_name("colmax")

        # eliminamos la extension .nc
        filename = Path(filename).stem

        # Verificamos el volúmen ---------------------------------------------------------------------------
        # Se determina si el volumen está completo o no, de acuerdo a las variables polarimétricas
        # esperadas, según lo detallado en vol_types.
        # Un volumen puede estar incompleto porque faltan archivos BUFR o porque son inconsistentes
        # (no se pueden convertir). Si hay un error en la conversion de algun campo, el volumen se marcara
        # como incompleto tambien. Este comportamiento es correcto, la falla en la integridad quedara
        # marcada en el campo error_type por lo que se reprocesara n veces tratando de procesarlo
        # correctamente o se terminara descartando.
        fields_to_check = vol_types[filename.split("_")[1]][filename.split("_")[2]][:]
        radar_fields = radar.fields.keys()
        missing_fields = set(fields_to_check) - set(radar_fields)
        # for field in radar.fields.keys():
        #     if field in fields_to_check:
        #         fields_to_check.remove(field)

        if missing_fields:
            # TODO: update database with incomplete volume status
            # update_unprocessed_vols_in_db2(radar_name, date=filename, condition=0)
            # update_vols_in_db2(radar_name, date=filename, condition=0,
            #                     error_type='Vol.Incompleto: ' + str(fields_to_check))
            # new_filename_list_mask[new_filename_list.index(filename.split('.')[0])] = 0
            logger.debug("Vol incompleto, faltan: " + str(missing_fields))

        else:
            # Si el vol esta completo se actualiza la condicion.
            # Esto es porque si el volumen fue marcado anteriormente por algun error, se reprocesaría
            # hasta descartarse aunque la condición de error desaparezca.
            # En cada iteracion, de esta forma, se inicializa la condicion en funcion si esta completo o
            # no, y luego se modifica o no dependiendo si se encuentran errores o no.

            # TODO: update database with complete volume status
            # update_vols_in_db2(radar_name, date=filename, condition=1)
            # new_filename_list_mask[new_filename_list.index(filename.split('.')[0])] = 1
            logger.debug("Vol completo.")

        # --- Generate COLMAX -----------------------------------------------------------
        if add_colmax:
            logger.debug(f"Gen.COLMAX {filename}")
            try:
                radar = generate_colmax(
                    radar=radar,
                    elev_limit1=config.COLMAX_ELEV_LIMIT1,
                    field_for_colmax=hrefl_field,
                    RHOHV_filter=config.COLMAX_RHOHV_FILTER,
                    RHOHV_umbral=config.COLMAX_RHOHV_UMBRAL,
                    WRAD_filter=config.COLMAX_WRAD_FILTER,
                    WRAD_umbral=config.COLMAX_WRAD_UMBRAL,
                    TDR_filter=config.COLMAX_TDR_FILTER,
                    TDR_umbral=config.COLMAX_TDR_UMBRAL,
                    logger_name="WEBMET." + radar.metadata["instrument_name"],
                    save_changes=True,
                )
                logger.debug(f"COLMAX generado correctamente para {filename}.")
            except Exception as e:
                logger.error("Generando COLMAX: " + filename + ": " + str(e))
                # TODO: update database with error status
                # update_vols_in_db2(radar_name, date=filename, condition=0, integrity='FALLA',
                #                     error_type='Generando_COLMAX', nprocess=1, logger_name=logger_name)

        # --- Prepare plotting lists ----------------------------------------------------
        field_plotted = False
        fields_to_plot = config.FIELDS_TO_PLOT
        plotted_fields = [f for f in fields_to_plot if f in radar.fields]

        # --- Plotting block (unfiltered) ----------------------------------------------
        # Plot configuration
        plot_config = RadarPlotConfig(figsize=(15, 15), dpi=config.PNG_DPI, transparent=True)
        import matplotlib.pyplot as plt

        plt.ioff()
        try:
            # placeholder for Py-ART RadarDisplay usage; delegate actual plotting to ctx.plot_save_ppi
            for field in list(plotted_fields):
                # special mapping for reflectivity raw/renamed
                if field in (hrefl_field, hrefl_field_raw):
                    plot_field = hrefl_field_raw
                elif field in (vrefl_field, vrefl_field_raw):
                    plot_field = vrefl_field_raw
                else:
                    plot_field = field

                if plot_field not in radar.fields:
                    continue

                try:
                    if field in [hrefl_field, vrefl_field, colmax_field]:
                        key_field = "REFL"
                    else:
                        key_field = plot_field
                    vmin_key = f"VMIN_{key_field}_NOFILTERS"
                    vmax_key = f"VMAX_{key_field}_NOFILTERS"
                    cmap_key = f"CMAP_{key_field}_NOFILTERS"
                    vmin = config.__dict__.get(vmin_key, None)
                    vmax = config.__dict__.get(vmax_key, None)
                    cmap = config.__dict__.get(cmap_key, None)

                    sweep = get_lowest_nsweep(radar)
                    field_config = FieldPlotConfig(plot_field, sweep=sweep)
                    # datetime = get_time_from_RMA_filename(filename)
                    fig, ax = plot_ppi_field(radar, field, sweep=sweep, config=plot_config, field_config=field_config)
                    try:
                        output_dict = product_path_and_filename(
                            radar, plot_field, sweep, round_filename=True, filtered=False
                        )
                        _ = save_ppi_png(
                            fig,
                            output_dict["ceiled"][0],
                            output_dict["ceiled"][1],
                            dpi=plot_config.dpi,
                            transparent=plot_config.transparent,
                        )
                        _ = save_ppi_png(
                            fig,
                            output_dict["rounded"][0],
                            output_dict["rounded"][1],
                            dpi=plot_config.dpi,
                            transparent=plot_config.transparent,
                        )

                        plt.close(fig)
                        field_plotted = True
                    except Exception as e:
                        logger.error(f"Generando path y filename para {plot_field}o: {e}")
                        # TODO: update database with error status
                        continue
                except Exception as e:
                    logger.error(f"Graficando {filename} | {plot_field}o: {e}")
                    # TODO: update database with error status
                    continue

                finally:
                    plt.clf()
                    gc.collect()
        except Exception as e:
            logger.error(f"Error General graficando: {e}")
            # ctx.update_vols_in_db2(ctx.radar_name, date=filename_no_ext, condition=0, integrity="FALLA",
            #                        error_type=f"Graficando General: {e}", nprocess=1)

        # --- Plotting block (filtered) ----------------------------------------------
        filtered_fields_to_plot = config.FILTERED_FIELDS_TO_PLOT
        filtered_plotted_fields = [f for f in filtered_fields_to_plot if f in radar.fields]
        try:
            # placeholder for Py-ART RadarDisplay usage; delegate actual plotting to ctx.plot_save_ppi
            for field in list(filtered_plotted_fields):
                plot_field = field
                if plot_field not in radar.fields:
                    continue

                try:
                    gatefilter = pyart.correct.GateFilter(radar)
                    if field in [colmax_field]:
                        gatefilter.exclude_below(colmax_field, config.COLMAX_THRESHOLD)
                    elif field in [
                        hrefl_field,
                        vrefl_field,
                        rhv_field,
                        phidp_field,
                        kdp_field,
                        zdr_field,
                        wrad_field,
                        vrad_field,
                    ]:
                        size = int(19000 / radar.range["meters_between_gates"])
                        gatefilter = filter_fields_grc1(
                            radar,
                            rhv_field=rhv_field,
                            rhv_filter1=config.GRC_RHV_FILTER,
                            rhv_threshold1=config.GRC_RHV_THRESHOLD,
                            wrad_field=wrad_field,
                            wrad_filter=config.GRC_WRAD_FILTER,
                            wrad_threshold=config.GRC_WRAD_THRESHOLD,
                            refl_field=hrefl_field,
                            refl_filter=config.GRC_REFL_FILTER,
                            refl_threshold=config.GRC_REFL_THRESHOLD,  # type: ignore
                            zdr_field=zdr_field,
                            zdr_filter=config.GRC_ZDR_FILTER,
                            zdr_threshold=config.GRC_ZDR_THRESHOLD,
                            refl_filter2=config.GRC_REFL_FILTER2,
                            refl_threshold2=config.GRC_REFL_THRESHOLD2,  # type: ignore
                            cm_field=cm_field,
                            cm_filter=config.GRC_CM_FILTER,
                            rhohv_threshold2=config.GRC_RHOHV_THRESHOLD2,
                            despeckle_filter=config.GRC_DESPECKLE_FILTER,
                            size=size,
                            mean_filter=config.GRC_MEAN_FILTER,
                            mean_threshold=config.GRC_MEAN_THRESHOLD,
                            target_fields=[hrefl_field],
                            overwrite_fields=False,
                            logger_name=logger_name,
                        )

                    sweep = get_lowest_nsweep(radar)
                    if field in [hrefl_field, vrefl_field, colmax_field]:
                        key_field = "REFL"
                    else:
                        key_field = plot_field
                    vmin_key = f"VMIN_{key_field}"
                    vmax_key = f"VMAX_{key_field}"
                    cmap_key = f"CMAP_{key_field}"
                    vmin = config.__dict__.get(vmin_key, None)
                    vmax = config.__dict__.get(vmax_key, None)
                    cmap = config.__dict__.get(cmap_key, None)

                    field_config = FieldPlotConfig(plot_field, vmin=vmin, vmax=vmax, cmap=cmap, sweep=sweep)
                    # datetime = get_time_from_RMA_filename(filename)
                    fig, ax = plot_ppi_field(radar, field, sweep=sweep, config=plot_config, field_config=field_config)
                    try:
                        output_dict = product_path_and_filename(
                            radar, plot_field, sweep, round_filename=True, filtered=True
                        )
                        _ = save_ppi_png(
                            fig,
                            output_dict["ceiled"][0],
                            output_dict["ceiled"][1],
                            dpi=plot_config.dpi,
                            transparent=plot_config.transparent,
                        )
                        _ = save_ppi_png(
                            fig,
                            output_dict["rounded"][0],
                            output_dict["rounded"][1],
                            dpi=plot_config.dpi,
                            transparent=plot_config.transparent,
                        )

                        plt.close(fig)
                        field_plotted = True
                    except Exception as e:
                        logger.error(f"Generando path y filename para {plot_field}: {e}")
                        # TODO: update database with error status
                        continue
                except Exception as e:
                    logger.error(f"Graficando {filename} | {plot_field}: {e}")
                    # TODO: update database with error status
                    continue

                finally:
                    plt.clf()
                    gc.collect()
        except Exception as e:
            logger.error(f"Error General graficando: {e}")
            # ctx.update_vols_in_db2(ctx.radar_name, date=filename_no_ext, condition=0, integrity="FALLA",
            #                        error_type=f"Graficando General: {e}", nprocess=1)
            fig.clf()
            plt.close(fig)
            # pylab.close(fig)
            plt.close("all")
            gc.collect()

    except Exception as e:
        logger.error(f"Error Procesando {filename}. {e}")
        # TODO: update database with error status

        # ctx.update_vols_in_db2(ctx.radar_name, date=os.path.splitext(filename)[0], integrity="FALLA",
        #                        condition=ctx.new_filename_list_mask[ctx.new_filename_list_mask.index(os.path.splitext(filename)[0])]
        #                        if os.path.splitext(filename)[0] in ctx.new_filename_list_mask else 0,
        #                        error_type=f"Procesando General: {e}", nprocess=1)
    finally:
        del radar
        gc.collect()

    if field_plotted:
        # vol_condition = get_vol_condition(db_name=radar_name, date=filename)
        # TODO: chequea condicion del volumen en la base de datos
        vol_condition = True  # placeholder

        if vol_condition:
            logger.info("____OK____ " + filename)
        else:
            logger.info("Procesamiento Finalizado con Errores (pendiente de reprocesamiento). " + filename)


def determine_reflectivity_fields(radar):
    """
    Determine the horizontal and vertical reflectivity fields from the radar object.

    Parameters
    ----------
    radar : Radar
        Py-ART Radar object containing fields.

    Returns
    -------
    dict
        A dictionary with the following keys:
        - 'hrefl_field': Horizontal reflectivity field (e.g., 'DBZH').
        - 'hrefl_field_raw': Raw horizontal reflectivity field (e.g., 'TH').
        - 'vrefl_field': Vertical reflectivity field (e.g., 'DBZV').
        - 'vrefl_field_raw': Raw vertical reflectivity field (e.g., 'TV').
    """
    # Initialize default values
    hrefl_field = hrefl_field_raw = "DBZH"
    vrefl_field = vrefl_field_raw = "DBZV"

    # Determine horizontal reflectivity fields
    if "DBZH" in radar.fields and "TH" in radar.fields:
        hrefl_field_raw = "TH"
    elif "DBZH" in radar.fields:
        hrefl_field = hrefl_field_raw = "DBZH"
    elif "TH" in radar.fields:
        hrefl_field = hrefl_field_raw = "TH"
    else:
        logger.warning("Campo de reflectividad horizontal inexistente.")

    # Determine vertical reflectivity fields
    if "DBZV" in radar.fields and "TV" in radar.fields:
        vrefl_field, vrefl_field_raw = "DBZV", "TV"
    elif "DBZV" in radar.fields:
        vrefl_field = vrefl_field_raw = "DBZV"
    elif "TV" in radar.fields:
        vrefl_field = vrefl_field_raw = "TV"
    else:
        logger.warning("Campo de reflectividad vertical inexistente.")

    return {
        "hrefl_field": hrefl_field,
        "hrefl_field_raw": hrefl_field_raw,
        "vrefl_field": vrefl_field,
        "vrefl_field_raw": vrefl_field_raw,
    }


def product_path_and_filename(radar, field, sweep, round_filename=True, filtered=True):
    radar_name = radar.metadata["instrument_name"]
    root_out = config.root_products

    # non-filtered fields have 'o' suffix
    if not filtered:
        field = f"{field}o"

    fnames_dict = {}
    try:
        if round_filename:
            date = get_time_from_RMA_filename(radar.metadata["filename"])
            cdate = date + datetime.timedelta(seconds=600)
            cdate = cdate.strftime("%Y%m%dT%H%M")[:-1] + "000Z"  # ceiled date
            rounded_min = str(round(date.minute / 10) * 10).zfill(2)
            rdate = f"{date.strftime('%Y%m%dT%H')}{rounded_min}00Z"  # rounded date

            filename_out = f"{radar_name}_{cdate}_{field}_{str(sweep).zfill(2)}.png"
            full_path = os.path.join(root_out, radar_name, rdate[:4], rdate[4:6], rdate[6:8])

            filename_out2 = f"{radar_name}_{rdate}_{field}_{str(sweep).zfill(2)}.png"
            full_path2 = os.path.join(root_out, radar_name, rdate[:4], rdate[4:6], rdate[6:8])

            # return full_path, filename_out, full_path2, filename_out2
            fnames_dict["ceiled"] = (full_path, filename_out)
            fnames_dict["rounded"] = (full_path2, filename_out2)
        else:
            elev = str(radar.get_elevation(sweep)[0])
            filename_out = f"{radar_name}_{elev}_{field}.png"
            full_path = os.path.join(root_out, radar_name, field)

            fnames_dict["non_rounded"] = (full_path, filename_out)
    except Exception as e:
        logger.error(f"Error generating product path and filename: {e}")

    return fnames_dict


if __name__ == "__main__":
    import os

    # Example usage
    example_filename = os.path.join(config.root_project, "outputs/example_netcdfs/RMA11_0315_01_20251020T151109Z.nc")
    volume_types = {
        "0315": {
            "01": ["DBZH", "DBZV", "ZDR", "RHOHV", "PHIDP", "KDP"],
            "02": ["VRAD", "WRAD"],
        },
    }
    process_volume(example_filename, volume_types, add_colmax=True)
