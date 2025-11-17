"""
Legacy BUFR decoder implementation.

This is the original implementation of the BUFR decoder using the C library
libdecbufr. Kept here for comparison and regression testing.

Note: Uses relative paths to C library which may need adjustment.
"""

import logging
import struct
import zlib
from ctypes import POINTER, Structure, c_char_p, c_double, c_int, cdll

import numpy as np

from radarlib import config


class point_t(Structure):
    _fields_ = [
        ("lat", c_double),
        ("lon", c_double),
    ]


class meta_t(Structure):
    _fields_ = [
        ("year", c_int),
        ("month", c_int),
        ("day", c_int),
        ("hour", c_int),
        ("min", c_int),
        ("radar", point_t),
        ("radar_height", c_double),
    ]


def dec_bufr_file(bufr_filename=None, logger_name=None):
    """Legacy BUFR file decoder using C library."""
    run_log = []

    if logger_name is None:
        logger = logging.getLogger(__name__)
    else:
        logger = logging.getLogger(logger_name)

    try:
        product_type = bufr_filename.split("/")[-1].split(".")[0].split("_")[3]

        root_bufr_resources = config.BUFR_RESOURCES_PATH + "/"
        root_bufr_tables = root_bufr_resources + "bufr_tables/"

        root_bufr_dynamic_library = root_bufr_resources + "dynamic_library/libdecbufr.so"
        libdecbufr = cdll.LoadLibrary(root_bufr_dynamic_library)

        # ======================================================================
        # DECBUFR get_meta_data
        # ======================================================================
        get_meta_data = libdecbufr.get_meta_data
        get_meta_data.argtypes = [c_char_p, c_char_p]
        get_meta_data.restype = POINTER(meta_t)
        metadata = get_meta_data(bufr_filename.encode("utf-8"), root_bufr_tables.encode("utf-8"))
        vol_metadata = {}
        vol_metadata["year"] = metadata.contents.year
        vol_metadata["month"] = metadata.contents.month
        vol_metadata["day"] = metadata.contents.day
        vol_metadata["hour"] = metadata.contents.hour
        vol_metadata["min"] = metadata.contents.min
        vol_metadata["lat"] = metadata.contents.radar.lat
        vol_metadata["lon"] = metadata.contents.radar.lon
        vol_metadata["radar_height"] = metadata.contents.radar_height

        # ======================================================================
        # DECBUFR get_size_data
        # ======================================================================
        get_size_data = libdecbufr.get_size_data
        get_size_data.argtypes = [c_char_p, c_char_p]
        get_size_data.restype = c_int
        size_data = get_size_data(bufr_filename.encode("utf-8"), root_bufr_tables.encode("utf-8"))

        # ======================================================================
        # DECBUFR get_elevation_data
        # ======================================================================
        get_elevation_data = libdecbufr.get_elevation_data
        get_elevation_data.argtypes = [c_char_p, c_char_p]
        get_elevation_data.restype = POINTER(c_double * 30)
        elevation_data = get_elevation_data(bufr_filename.encode("utf-8"), root_bufr_tables.encode("utf-8"))

        elevation_data = list(elevation_data.contents)
        elevation_data = np.asarray(elevation_data)

        # ======================================================================
        # DECBUFR get_data_part
        # ======================================================================
        get_data_part = libdecbufr.get_data
        get_data_part.argtypes = [c_char_p, c_char_p]
        array = c_int * size_data
        get_data_part.restype = POINTER(array)
        vol_data = get_data_part(bufr_filename.encode("utf-8"), root_bufr_tables.encode("utf-8"))

        vol = list(vol_data.contents)
        vol = np.asarray(vol)
        del vol_data

        # =====================================================================
        # Parser
        # =====================================================================
        sweeps_data = []
        nsweeps = int(vol[0])
        vol_metadata["nsweeps"] = nsweeps

        u = 1
        for sweep in range(nsweeps):
            barrido = {}
            barrido["compress_data"] = []

            # PARSEO ENCABEZADOS
            barrido["year_ini"] = vol[u]
            u = u + 1
            barrido["month_ini"] = vol[u]
            u = u + 1
            barrido["day_ini"] = vol[u]
            u = u + 1
            barrido["hour_ini"] = vol[u]
            u = u + 1
            barrido["min_ini"] = vol[u]
            u = u + 1
            barrido["sec_ini"] = vol[u]
            u = u + 1
            barrido["year"] = vol[u]
            u = u + 1
            barrido["month"] = vol[u]
            u = u + 1
            barrido["day"] = vol[u]
            u = u + 1
            barrido["hour"] = vol[u]
            u = u + 1
            barrido["min"] = vol[u]
            u = u + 1
            barrido["sec"] = vol[u]
            u = u + 1
            barrido["Type_of_product"] = vol[u]
            u = u + 1
            barrido["elevation"] = elevation_data[sweep]
            u = u + 1
            barrido["ngates"] = vol[u]
            u = u + 1
            barrido["range_size"] = vol[u]
            u = u + 1
            barrido["range_offset"] = vol[u]
            u = u + 1
            barrido["nrays"] = vol[u]
            u = u + 1
            barrido["antenna_beam_az"] = vol[u]
            u = u + 2
            barrido["Type of product"] = vol[u]
            u = u + 2

            # PARSEO DE DATOS
            multi_pri = vol[u]
            u = u + 1
            for _i in range(multi_pri):
                multi_sec = vol[u]
                u = u + 1
                for _j in range(multi_sec):
                    if vol[u] == 99999:
                        barrido["compress_data"].append(255)
                    else:
                        barrido["compress_data"].append(vol[u])
                    u = u + 1

            barrido["compress_data"] = bytearray(barrido["compress_data"])
            sweeps_data.append(barrido)

        # ======================================================================
        # Descompresion
        # ======================================================================
        for sweep in range(nsweeps):
            sweeps_data[sweep]["data"] = []

            # if sys.version_info[0] < 3:
            #     data_buf = buffer(sweeps_data[sweep]["compress_data"])
            # else:
            data_buf = memoryview(sweeps_data[sweep]["compress_data"])

            dec_data = zlib.decompress(data_buf)
            dec_data_bytes = bytearray(dec_data)

            j = 0
            for _i in range(int(np.size(dec_data_bytes) / 8)):
                sweeps_data[sweep]["data"].append(struct.unpack("d", dec_data_bytes[j : j + 8])[0])
                j = j + 8

        # =====================================================================
        # Adecuacion de los Datos
        # =====================================================================
        sweeps_to_remove = []
        for sweep in range(nsweeps):
            if sweeps_data[sweep]["ngates"] > 8400:
                vol_name = bufr_filename.split("/")[-1].split(".")[0][:-5]
                logger.warning(
                    str(vol_name)
                    + ": se descarta Barrido Inconsistente ("
                    + product_type
                    + " / Sw: "
                    + str(sweep)
                    + ") (ngates fuera limites)"
                )
                run_log.append(
                    [
                        2,
                        "Barrido Inconsistente (" + product_type + " / Sw: " + str(sweep) + ") (ngates fuera limites)",
                    ]
                )
                sweeps_to_remove.append(sweep)

        for sweep in sweeps_to_remove:
            del sweeps_data[sweep]

        vol_metadata["nsweeps"] = len(sweeps_data)
        nsweeps = len(sweeps_data)

        # Enmascarado los valores 'Missing Values'
        for sweep in range(nsweeps):
            sweeps_data[sweep]["data"] = np.ma.masked_equal(sweeps_data[sweep]["data"], -1.797693134862315708e308)

        # Redimensionamos los datos
        for sweep in range(nsweeps):
            sweeps_data[sweep]["data"] = np.reshape(
                sweeps_data[sweep]["data"],
                (sweeps_data[sweep]["nrays"], sweeps_data[sweep]["ngates"]),
            )

        # Los ngates de todos los sweeps deben ser iguales
        ngates_max = []
        for sweep in range(nsweeps):
            ngates_max.append(int(sweeps_data[sweep]["ngates"]))
        ngates_max = int(np.max(ngates_max))

        for sweep in range(0, nsweeps):
            ngates = int(sweeps_data[sweep]["ngates"])
            nrays = int(sweeps_data[sweep]["nrays"])

            if ngates != ngates_max:
                sw_data_cp = sweeps_data[sweep]["data"].copy()
                sweeps_data[sweep]["data"] = np.empty((nrays, ngates_max))
                sweeps_data[sweep]["data"][:] = np.nan

                sweeps_data[sweep]["data"][:, 0:ngates] = sw_data_cp[:, :]

        # Reacomodo la info para formato PyART
        vol_data = sweeps_data[0]["data"]
        for sweep in range(nsweeps - 1):
            vol_data = np.concatenate((vol_data, sweeps_data[sweep + 1]["data"]), axis=0)

        return vol_metadata, sweeps_data, vol_data, run_log

    except Exception as e:
        raise ValueError(product_type + ": Error General en dec_bufr_file: " + str(e))


def bufr_to_dict(filename, path, debug, volumenes, run_logs, logger_name=None):
    """Legacy BUFR to dict converter."""
    if logger_name is None:
        logger = logging.getLogger(__name__ + "." + filename.split("_")[0])
    else:
        logger = logging.getLogger(logger_name + "." + filename.split("_")[0])

    try:
        if debug:
            logger.debug("Procesando: " + filename)

        vol = {}

        [meta_data_vol, meta_data_sweeps, vol_data, run_log] = dec_bufr_file(
            bufr_filename=path + filename, logger_name=logger_name
        )

        if run_log:
            run_logs.append(run_log)

        vol["metadata_vol"] = meta_data_vol
        vol["metadata_vol"]["radar_name"] = filename.split("_")[0]
        vol["metadata_vol"]["estrategia_nombre"] = filename.split("_")[1]
        vol["metadata_vol"]["estrategia_nvol"] = filename.split("_")[2]
        vol["metadata_vol"]["tipo_producto"] = filename.split("_")[3]
        vol["metadata_vol"]["filename"] = filename
        vol["metadata_sweeps"] = meta_data_sweeps

        vol["data"] = vol_data

        # =====================================================================
        # INFO VOLUMEN
        # =====================================================================
        vol["info"] = {}
        vol["info"]["estrategia"] = {}
        vol["info"]["metadata"] = {}

        vol["info"]["nombre_radar"] = vol["metadata_vol"]["radar_name"]
        vol["info"]["estrategia"]["nombre"] = vol["metadata_vol"]["estrategia_nombre"]
        vol["info"]["estrategia"]["volume_number"] = vol["metadata_vol"]["estrategia_nvol"]
        vol["info"]["tipo_producto"] = vol["metadata_vol"]["tipo_producto"]
        vol["info"]["filename"] = vol["metadata_vol"]["filename"]

        vol["info"]["ano_vol"] = vol["metadata_vol"]["year"]
        vol["info"]["mes_vol"] = vol["metadata_vol"]["month"]
        vol["info"]["dia_vol"] = vol["metadata_vol"]["day"]
        vol["info"]["hora_vol"] = vol["metadata_vol"]["hour"]
        vol["info"]["min_vol"] = vol["metadata_vol"]["min"]
        vol["info"]["lat"] = vol["metadata_vol"]["lat"]
        vol["info"]["lon"] = vol["metadata_vol"]["lon"]
        vol["info"]["altura"] = vol["metadata_vol"]["radar_height"]
        vol["info"]["nsweeps"] = vol["metadata_vol"]["nsweeps"]

        nsweeps = vol["info"]["nsweeps"]
        vol["info"]["ano_sweep_ini"] = np.zeros(nsweeps)
        vol["info"]["mes_sweep_ini"] = np.zeros(nsweeps)
        vol["info"]["dia_sweep_ini"] = np.zeros(nsweeps)
        vol["info"]["hora_sweep_ini"] = np.zeros(nsweeps)
        vol["info"]["min_sweep_ini"] = np.zeros(nsweeps)
        vol["info"]["seg_sweep_ini"] = np.zeros(nsweeps)
        vol["info"]["ano_sweep"] = np.zeros(nsweeps)
        vol["info"]["mes_sweep"] = np.zeros(nsweeps)
        vol["info"]["dia_sweep"] = np.zeros(nsweeps)
        vol["info"]["hora_sweep"] = np.zeros(nsweeps)
        vol["info"]["min_sweep"] = np.zeros(nsweeps)
        vol["info"]["seg_sweep"] = np.zeros(nsweeps)
        vol["info"]["elevaciones"] = np.zeros(nsweeps)
        vol["info"]["ngates"] = np.zeros(nsweeps)
        vol["info"]["gate_size"] = np.zeros(nsweeps)
        vol["info"]["gate_offset"] = np.zeros(nsweeps)
        vol["info"]["nrayos"] = np.zeros(nsweeps)
        vol["info"]["rayo_inicial"] = np.zeros(nsweeps)

        for sweep in range(0, nsweeps):
            vol["info"]["ano_sweep_ini"][sweep] = int(vol["metadata_sweeps"][sweep]["year_ini"])
            vol["info"]["mes_sweep_ini"][sweep] = int(vol["metadata_sweeps"][sweep]["month_ini"])
            vol["info"]["dia_sweep_ini"][sweep] = int(vol["metadata_sweeps"][sweep]["day_ini"])
            vol["info"]["hora_sweep_ini"][sweep] = int(vol["metadata_sweeps"][sweep]["hour_ini"])
            vol["info"]["min_sweep_ini"][sweep] = int(vol["metadata_sweeps"][sweep]["min_ini"])
            vol["info"]["seg_sweep_ini"][sweep] = int(vol["metadata_sweeps"][sweep]["sec_ini"])
            vol["info"]["ano_sweep"][sweep] = int(vol["metadata_sweeps"][sweep]["year"])
            vol["info"]["mes_sweep"][sweep] = int(vol["metadata_sweeps"][sweep]["month"])
            vol["info"]["dia_sweep"][sweep] = int(vol["metadata_sweeps"][sweep]["day"])
            vol["info"]["hora_sweep"][sweep] = int(vol["metadata_sweeps"][sweep]["hour"])
            vol["info"]["min_sweep"][sweep] = int(vol["metadata_sweeps"][sweep]["min"])
            vol["info"]["seg_sweep"][sweep] = int(vol["metadata_sweeps"][sweep]["sec"])

            vol["info"]["elevaciones"][sweep] = float(vol["metadata_sweeps"][sweep]["elevation"])
            vol["info"]["ngates"][sweep] = int(vol["metadata_sweeps"][sweep]["ngates"])
            vol["info"]["gate_size"][sweep] = int(vol["metadata_sweeps"][sweep]["range_size"])
            vol["info"]["gate_offset"][sweep] = int(vol["metadata_sweeps"][sweep]["range_offset"])
            vol["info"]["nrayos"][sweep] = int(vol["metadata_sweeps"][sweep]["nrays"])
            vol["info"]["rayo_inicial"][sweep] = int(vol["metadata_sweeps"][sweep]["antenna_beam_az"])

        vol["info"]["metadata"]["comment"] = "-"
        vol["info"]["metadata"]["instrument_type"] = "Radar"
        vol["info"]["metadata"]["site_name"] = "-"
        vol["info"]["metadata"]["Sub_conventions"] = "-"
        vol["info"]["metadata"]["references"] = "-"
        vol["info"]["metadata"]["volume_number"] = vol["info"]["estrategia"]["volume_number"]
        vol["info"]["metadata"]["scan_id"] = vol["info"]["estrategia"]["nombre"]
        vol["info"]["metadata"]["title"] = "-"
        vol["info"]["metadata"]["source"] = "-"
        vol["info"]["metadata"]["version"] = "-"
        vol["info"]["metadata"]["instrument_name"] = vol["info"]["nombre_radar"]
        vol["info"]["metadata"]["ray_times_increase"] = "-"
        vol["info"]["metadata"]["platform_is_mobile"] = "false"
        vol["info"]["metadata"]["driver"] = "-"
        vol["info"]["metadata"]["institution"] = "SiNaRaMe"
        vol["info"]["metadata"]["n_gates_vary"] = "-"
        vol["info"]["metadata"]["primary_axis"] = "-"
        vol["info"]["metadata"]["created"] = (
            "Fecha:"
            + str(int(vol["info"]["dia_sweep"][0]))
            + "/"
            + str(int(vol["info"]["mes_sweep"][0]))
            + "/"
            + str(int(vol["info"]["ano_sweep"][0]))
            + " Hora:"
            + str(int(vol["info"]["hora_sweep"][0]))
            + ":"
            + str(int(vol["info"]["min_sweep"][0]))
            + ":"
            + str(int(vol["info"]["seg_sweep"][0]))
        )
        vol["info"]["metadata"]["scan_name"] = "-"
        vol["info"]["metadata"]["author"] = "Grupo Radar Cordoba (GRC) - " + "Extractor/Conversor de Datos de Radar "
        vol["info"]["metadata"]["Conventions"] = "-"
        vol["info"]["metadata"]["platform_type"] = "Base Fija"
        vol["info"]["metadata"]["history"] = "-"
        vol["info"]["metadata"]["filename"] = (
            filename.split("_")[0]
            + "_"
            + filename.split("_")[1]
            + "_"
            + filename.split("_")[2]
            + "_"
            + filename.split("_")[4].split(".")[0]
            + ".nc"
        )

        del vol["metadata_sweeps"]
        del vol["metadata_vol"]

        # Compatibility checks
        for sweep in range(1, vol["info"]["nsweeps"]):
            if vol["info"]["nrayos"][0] != vol["info"]["nrayos"][sweep]:
                raise ValueError("Volumen no soportado, número de " + "rayos distintos entre sweeps")

            if vol["info"]["gate_offset"][0] != vol["info"]["gate_offset"][sweep]:
                gate_size = vol["info"]["gate_size"][0]
                max_offset = int(gate_size / 2)
                if abs(vol["info"]["gate_offset"][0] - vol["info"]["gate_offset"][sweep]) < max_offset:
                    if debug:
                        logger.warning(
                            "Desplazamiento de Sweeps, gates "
                            + "iniciales no coincidentes. "
                            + "Gate_offset Ref: "
                            + str(vol["info"]["gate_offset"][0])
                            + " | Gate_offset (Sweep: "
                            + str(sweep)
                            + "): "
                            + str(vol["info"]["gate_offset"][sweep])
                        )
                else:
                    raise ValueError("Volumen no soportado, valores " + "de gate_offset distintos entre sweeps")

            if vol["info"]["gate_size"][0] != vol["info"]["gate_size"][sweep]:
                raise ValueError("Volumen no soportado, valores de " + "gate_size  distintos entre sweeps")

        volumenes.append(vol)

        if debug:
            logger.debug("Finalizado: " + filename)

    except Exception as e:
        logger.error("Error en bufr_to_dict: ")
        logger.error("Detalle Error: " + str(e))
        run_logs.append([3, str(e)])
