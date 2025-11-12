from __future__ import annotations

from pathlib import Path
from typing import Any, Dict
from xml.dom import minidom


def read_xml_estrategia2(full_path_xml: str | Path, nvol: int = 0, nsweep: int = 0) -> Dict[str, Any]:
    xmldoc = minidom.parse(str(full_path_xml))
    vol_list = xmldoc.getElementsByTagName("volumen")

    scan_strategy = vol_list[nsweep].attributes["tipo"].value
    longitud_celda = vol_list[nsweep].attributes["longitud_celda_m"].value

    sweep_list = vol_list[nvol].getElementsByTagName("procesamiento")
    processing_type = sweep_list[nsweep].attributes["tipo"].value

    info: Dict[str, Any] = {}
    info["scan_strategy"] = scan_strategy
    info["processing_type"] = processing_type
    info["longitud_celda"] = longitud_celda

    if processing_type in ["intensidad", "doppler", "surv", "doppler-sfr"]:
        group_list = sweep_list[nsweep].getElementsByTagName("barrido")[0].getElementsByTagName("grupo")
        npulsos = group_list[0].attributes["pulsos"].value
        prp1 = group_list[0].attributes["prp_us"].value
        pw1 = group_list[0].attributes["pw_ns"].value
        max_range1 = group_list[0].attributes["alcance_km"].value

        info.update(
            {
                "prp1": int(prp1),
                "pw1": int(pw1),
                "max_range1": int(max_range1),
                "npulses": int(npulsos),
                "prp2": 0,
                "pw2": 0,
                "max_range2": 0,
                "nconjuntos": 0,
            }
        )

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

        info.update(
            {
                "prp1": int(prp1),
                "pw1": int(pw1),
                "max_range1": int(max_range1),
                "npulses": 0,
                "prp2": int(prp2),
                "pw2": int(pw2),
                "max_range2": int(max_range2),
                "nconjuntos": int(nconjuntos),
            }
        )

        return info

    return info
