"""
Legacy BUFR to PyART conversion implementation.

This is the original monolithic implementation that converts BUFR-decoded
data to PyART Radar objects. Kept for comparison and regression testing.
"""

import datetime
import logging
from typing import Any, Dict, List, Optional

import numpy as np

try:
    import pyart
except ImportError:
    pyart = None


def read_xml_estrategia2(full_path_xml: str, nvol: int = 0, nsweep: int = 0) -> Dict[str, Any]:
    """
    Legacy XML scan strategy reader.

    Parameters
    ----------
    full_path_xml : str
        Full path to the XML file
    nvol : int
        Volume number (0-indexed)
    nsweep : int
        Sweep number (0-indexed)

    Returns
    -------
    Dict[str, Any]
        Dictionary with scan strategy information
    """
    from xml.dom import minidom

    xmldoc = minidom.parse(full_path_xml)
    vol_list = xmldoc.getElementsByTagName("volumen")

    scan_strategy = vol_list[nsweep].attributes["tipo"].value
    longitud_celda = vol_list[nsweep].attributes["longitud_celda_m"].value

    sweep_list = vol_list[nvol].getElementsByTagName("procesamiento")
    processing_type = sweep_list[nsweep].attributes["tipo"].value

    if processing_type in ["intensidad", "doppler", "surv", "doppler-sfr"]:
        group_list = sweep_list[nsweep].getElementsByTagName("barrido")[0].getElementsByTagName("grupo")
        npulsos = group_list[0].attributes["pulsos"].value
        prp1 = group_list[0].attributes["prp_us"].value
        pw1 = group_list[0].attributes["pw_ns"].value
        max_range1 = group_list[0].attributes["alcance_km"].value

        info = {}
        info["scan_strategy"] = scan_strategy
        info["processing_type"] = processing_type
        info["longitud_celda"] = longitud_celda
        info["prp1"] = int(prp1)
        info["pw1"] = int(pw1)
        info["max_range1"] = int(max_range1)
        info["npulses"] = int(npulsos)
        info["prp2"] = 0
        info["pw2"] = 0
        info["max_range2"] = 0
        info["nconjuntos"] = 0
        return info

    if processing_type == "staggered":
        nconjuntos = sweep_list[nsweep].getElementsByTagName("barrido")[0].attributes["conjuntos"].value
        group_list = sweep_list[nsweep].getElementsByTagName("barrido")[0].getElementsByTagName("grupo")
        prp1 = group_list[0].attributes["prp_us"].value
        pw1 = group_list[0].attributes["pw_ns"].value
        max_range1 = group_list[0].attributes["alcance_km"].value
        prp2 = group_list[1].attributes["prp_us"].value
        pw2 = group_list[1].attributes["pw_ns"].value
        max_range2 = group_list[1].attributes["alcance_km"].value

        info = {}
        info["scan_strategy"] = scan_strategy
        info["processing_type"] = processing_type
        info["longitud_celda"] = longitud_celda
        info["prp1"] = int(prp1)
        info["pw1"] = int(pw1)
        info["max_range1"] = int(max_range1)
        info["npulsos"] = 0
        info["prp2"] = int(prp2)
        info["pw2"] = int(pw2)
        info["max_range2"] = int(max_range2)
        info["nconjuntos"] = int(nconjuntos)
        return info


def bufr_to_pyart_legacy(
    fields: List[Dict[str, Any]],
    debug: bool = False,
    logger_name: str = __name__,
    include_scan_metadata: bool = False,
    root_scan_config_files: Optional[str] = None,
) -> Any:
    """
    Legacy BUFR to PyART conversion.

    This is the original monolithic implementation that takes decoded BUFR
    fields and converts them to a PyART Radar object.

    Parameters
    ----------
    fields : List[Dict[str, Any]]
        List of decoded BUFR field dictionaries
    debug : bool, optional
        Debug logging flag
    logger_name : str, optional
        Logger name
    include_scan_metadata : bool, optional
        Include RMA scan metadata from XML files
    root_scan_config_files : str, optional
        Root directory for scan config XML files

    Returns
    -------
    pyart.Radar
        PyART Radar object
    """
    if pyart is None:
        raise ImportError("pyart is required for this function")

    logger = logging.getLogger(logger_name)

    try:
        if fields:
            # Define reference field (the one with farthest range)
            rango_maximo = np.zeros(len(fields))
            for i, field in enumerate(fields):
                rango_maximo[i] = (
                    field["info"]["gate_offset"][0] + field["info"]["gate_size"][0] * field["info"]["ngates"][0]
                )
            nreferencia = np.argmax(rango_maximo)

            # Load reference field data and info
            field = fields[nreferencia]

            field_name = field["info"]["tipo_producto"]

            # Create empty PyART Radar object
            radar = pyart.testing.make_empty_ppi_radar(
                int(field["info"]["ngates"][0]),
                int(field["info"]["nrayos"][0]),
                int(field["info"]["nsweeps"]),
            )

            # Add reference field
            radar.add_field(field_name, field, replace_existing=True)

            # Load altitude
            radar.altitude["data"] = np.ndarray(1)
            radar.altitude["data"][0] = field["info"]["altura"]
            radar.altitude["units"] = "metros"
            radar.altitude["long_name"] = "altitud"
            radar.altitude["possitive:"] = "arriba"
            radar.altitude["_fillValue"] = -9999.0

            # Load azimuth, elevation, fixed_angle
            z = 0
            for i in range(radar.nsweeps):
                for j in range(0, int(field["info"]["nrayos"][i])):
                    radar.azimuth["data"][z] = j
                    radar.elevation["data"][z] = field["info"]["elevaciones"][i]
                    z = z + 1
                radar.fixed_angle["data"][i] = field["info"]["elevaciones"][i]

            # Load geographic coordinates
            radar.latitude["data"] = np.ndarray(1)
            radar.latitude["data"][0] = field["info"]["lat"]
            radar.latitude["units"] = "grados"
            radar.latitude["long_name"] = "latitud"
            radar.latitude["_fillValue"] = -9999.0

            radar.longitude["data"] = np.ndarray(1)
            radar.longitude["data"][0] = field["info"]["lon"]
            radar.longitude["units"] = "grados"
            radar.longitude["long_name"] = "longitud"
            radar.longitude["_fillValue"] = -9999.0

            # Use precise coordinates from KML files if available
            radar_name = field["info"]["metadata"]["instrument_name"]
            coor_radares = {
                "AR1": (-34.78759000, -58.53660000),
                "AR5": (-33.94612000, -60.56260000),
                "AR7": (-31.84849000, -60.53724000),
                "AR8": (-36.53965000, -63.98984000),
                "RMA1": (-31.44133000, -64.19192000),
                "RMA2": (-34.80082000, -58.51557000),
                "RMA3": (-24.73028000, -60.55139000),
                "RMA4": (-27.45167000, -59.05083000),
                "RMA5": (-26.27812000, -53.67085000),
                "RMA6": (-37.91306000, -57.52783000),
                "RMA7": (-38.87662000, -68.14489000),
                "RMA12": (-41.13944000, -71.14944000),
            }
            if radar_name in coor_radares.keys():
                radar.latitude["data"][0] = coor_radares[radar_name][0]
                radar.longitude["data"][0] = coor_radares[radar_name][1]
            else:
                radar.latitude["data"][0] = field["info"]["lat"]
                radar.longitude["data"][0] = field["info"]["lon"]

            # Load range information
            gate_size = field["info"]["gate_size"][0]
            gate_offset = field["info"]["gate_offset"][0]

            radar.range["data"][0] = gate_offset
            for i in range(1, radar.ngates):
                radar.range["data"][i] = radar.range["data"][i - 1] + gate_size
                radar.range["data"] = np.array(radar.range["data"])

            radar.range["meters_between_gates"] = gate_size
            radar.range["meters_to_center_of_first_gate"] = gate_offset

            # Load metadata
            radar.metadata["comment"] = field["info"]["metadata"]["comment"]
            radar.metadata["instrument_type"] = field["info"]["metadata"]["instrument_type"]

            site_names = {
                "AR1": "Ezeiza",
                "AR5": "Pergamino",
                "AR7": "Parana",
                "AR8": "Anguil",
                "RMA1": "Cordoba",
                "RMA2": "Ezeiza",
                "RMA3": "Las Lomitas",
                "RMA4": "Resistencia",
                "RMA5": "Bernardo de Irigoyen",
                "RMA6": "Mar del Plata",
                "RMA7": "Neuquen",
                "RMA8": "Mercedes",
                "RMA9": "Río Grande",
                "RMA10": "Base Cte. Espora",
                "RMA11": "Termas de Río Hondo",
                "RMA12": "Bariloche",
            }
            if radar_name in site_names.keys():
                radar.metadata["site_name"] = site_names[radar_name]
            else:
                radar.metadata["site_name"] = ""

            radar.metadata["sub_conventions"] = ""
            radar.metadata["references"] = ""
            radar.metadata["title"] = ""
            radar.metadata["source"] = ""
            radar.metadata["version"] = ""
            radar.metadata["driver"] = ""
            radar.metadata["primary_axis"] = ""
            radar.metadata["scan_name"] = ""
            radar.metadata["Conventions"] = ""
            radar.metadata["history"] = ""

            radar.metadata["volume_number"] = field["info"]["metadata"]["volume_number"]
            radar.metadata["scan_id"] = field["info"]["metadata"]["scan_id"]
            radar.metadata["instrument_name"] = field["info"]["metadata"]["instrument_name"]

            radar.metadata["ray_times_increase"] = (
                "Fijo en cada elevacion. Se define en función de la velocidad de rotacion de antena"
            )

            radar.metadata["platform_is_mobile"] = field["info"]["metadata"]["platform_is_mobile"]

            radar.metadata["institution"] = field["info"]["metadata"]["institution"]

            radar.metadata["n_gates_vary"] = field["info"]["metadata"]["n_gates_vary"]

            radar.metadata["created"] = field["info"]["metadata"]["created"]

            radar.metadata["author"] = field["info"]["metadata"]["author"]

            radar.metadata["platform_type"] = field["info"]["metadata"]["platform_type"]

            radar.metadata["filename"] = field["info"]["metadata"]["filename"]

            # Load time information
            radar.time["comment"] = "tiempos relativos al tiempo de inicio del primer barrido del volumen"
            radar.time["long_name"] = "tiempo en segundos desde inicio del primer barrido del volumen"
            radar.time["standard_name"] = "tiempo"
            radar.time["units"] = (
                "seconds since "
                + str(int(field["info"]["ano_sweep"][0]))
                + "-"
                + str(int(field["info"]["mes_sweep"][0]))
                + "-"
                + str(int(field["info"]["dia_sweep"][0]))
                + "T"
                + str(int(field["info"]["hora_sweep"][0]))
                + ":"
                + str(int(field["info"]["min_sweep"][0]))
                + ":"
                + str(int(field["info"]["seg_sweep"][0]))
                + "Z"
            )

            # Reference time
            t_ref = datetime.datetime(
                int(field["info"]["ano_sweep_ini"][0]),
                int(field["info"]["mes_sweep_ini"][0]),
                int(field["info"]["dia_sweep_ini"][0]),
                int(field["info"]["hora_sweep_ini"][0]),
                int(field["info"]["min_sweep_ini"][0]),
                int(field["info"]["seg_sweep_ini"][0]),
            )
            radar.time["relative_initial_sweep_time"] = np.zeros(radar.nsweeps)
            radar.time["relative_final_sweep_time"] = np.zeros(radar.nsweeps)
            radar.time["initial_sweep_time"] = [""] * radar.nsweeps
            radar.time["final_sweep_time"] = [""] * radar.nsweeps

            for sweep in range(radar.nsweeps):
                hora_inicial = datetime.datetime(
                    int(field["info"]["ano_sweep_ini"][sweep]),
                    int(field["info"]["mes_sweep_ini"][sweep]),
                    int(field["info"]["dia_sweep_ini"][sweep]),
                    int(field["info"]["hora_sweep_ini"][sweep]),
                    int(field["info"]["min_sweep_ini"][sweep]),
                    int(field["info"]["seg_sweep_ini"][sweep]),
                )
                hora_final = datetime.datetime(
                    int(field["info"]["ano_sweep"][sweep]),
                    int(field["info"]["mes_sweep"][sweep]),
                    int(field["info"]["dia_sweep"][sweep]),
                    int(field["info"]["hora_sweep"][sweep]),
                    int(field["info"]["min_sweep"][sweep]),
                    int(field["info"]["seg_sweep"][sweep]),
                )

                radar.time["initial_sweep_time"][sweep] = hora_inicial.strftime("%Y%m%dT%H%M%S")
                radar.time["final_sweep_time"][sweep] = hora_final.strftime("%Y%m%dT%H%M%S")
                radar.time["relative_initial_sweep_time"][sweep] = (hora_inicial - t_ref).total_seconds()
                radar.time["relative_final_sweep_time"][sweep] = (hora_final - t_ref).total_seconds()

            # Ray times
            global_ray = 0
            for sweep in range(radar.nsweeps):
                tiempo_entre_rayos = (
                    radar.time["relative_final_sweep_time"][sweep] - radar.time["relative_initial_sweep_time"][sweep]
                ) / radar.rays_per_sweep["data"][sweep]

                for ray in range(0, int(field["info"]["nrayos"][sweep])):
                    radar.time["data"][global_ray] = (
                        radar.time["relative_initial_sweep_time"][sweep] + ray * tiempo_entre_rayos
                    )
                    global_ray = global_ray + 1

            # Radar instrument parameters
            radar.instrument_parameters = {}

            # Load field data
            for field in fields:
                ref_offset = radar.range["data"][0]
                ref_gate_size = radar.range["meters_between_gates"]
                field_offset = field["info"]["gate_offset"][0]
                field_gate_size = field["info"]["gate_size"][0]
                field_nrays = int(field["info"]["nrayos"][0])
                field_ngates = int(field["info"]["ngates"][0])
                field_name = field["info"]["tipo_producto"]

                # Set field units
                if field_name in ["TV", "TH", "DBZH", "DBZV", "ZDR", "TDR"]:
                    field["units"] = "dBZ"
                elif field_name in ["CM"]:
                    field["units"] = "masked"
                elif field_name in ["PhiDP"]:
                    field["units"] = "deg"
                elif field_name in ["KDP"]:
                    field["units"] = "deg/km"
                elif field_name in ["VRAD", "WRAD"]:
                    field["units"] = "m/s"

                # Align field dimensions if needed
                if field_ngates != radar.ngates:
                    if debug:
                        logger.debug(field_name + ": dimensión adaptada.")

                    data = field["data"].copy()
                    field["data"] = np.ma.masked_all((radar.nrays, radar.ngates))

                    if field_offset == ref_offset and field_gate_size == ref_gate_size:
                        for ray in range(field_nrays):
                            for gate in range(field_ngates):
                                field["data"][ray, gate] = data[ray, gate]

                    elif field_offset > ref_offset and field_gate_size == ref_gate_size:
                        init_gate = int((field_offset - ref_offset) / ref_gate_size)
                        for ray in range(field_nrays):
                            for gate in range(field_ngates):
                                field["data"][ray, init_gate + gate] = data[ray, gate]

                    else:
                        raise ValueError("Error al intentar acomodar los productos meteorologicos.")

                # Add field to radar
                del field["info"]
                field["data"] = field["data"].astype("float32")
                radar.add_field(field_name, field, replace_existing=True)

                # Mask invalid data
                radar.fields[field_name]["data"] = np.ma.masked_invalid(radar.fields[field_name]["data"])
                radar.fields[field_name]["data"] = np.ma.masked_outside(
                    radar.fields[field_name]["data"], -100000, 100000
                )

            return radar

    except Exception as e:
        logger.error("Error General en bufr_to_pyart: " + str(e))
        raise
