import datetime
import logging
import os
import re
from datetime import timezone
from typing import Dict, Optional

import pytz

from radarlib import config

tz_utc = pytz.timezone("UTC")
tz_arg = pytz.timezone("America/Argentina/Cordoba")


logger = logging.getLogger(__name__)


def get_time_from_RMA_filename(filename, tz_UTC=True):
    """
    Extract datetime from RMA BUFR filename.
    """
    str_time = filename.split("_")[3].split(".")[0]
    date = datetime.datetime.strptime(str_time, "%Y%m%dT%H%M%SZ")

    # el huso horario de los vols rma es UTC
    date = date.replace(tzinfo=timezone.utc)

    if not tz_UTC:
        # trasladamos tiempo a huso horario argentino
        date = date.astimezone(tz_arg)

    return date


def get_path_from_RMA_filename(filename, **kwargs):
    root_radar_files = kwargs.get("root_radar_files")
    if root_radar_files is None:
        root_radar_files = config.ROOT_RADAR_FILES_PATH

    radar = filename.split("_")[0]
    ano = filename.split("_")[3].split("T")[0][0:4]
    mes = filename.split("_")[3].split("T")[0][4:6]
    dia = filename.split("_")[3].split("T")[0][6:8]
    hora = filename.split("_")[3].split("T")[1][0:2]

    path = os.path.join(root_radar_files, radar, ano, mes, dia, hora)
    return path


def get_netcdf_filename_from_bufr_filename(ref_filename: str) -> str:
    """Generate netCDF filename from BUFR filename for RMA radars."""
    # Elimino la extensión original del archivo leido y armo el
    # nombre final por partes.
    fichero = ref_filename.split(".")[0]
    fichero = (
        fichero.split("_")[0] + "_" + fichero.split("_")[1] + "_" + fichero.split("_")[2] + "_" + fichero.split("_")[4]
    )
    return fichero + ".nc"


def extract_bufr_filename_components(filename: str) -> dict:
    """
    Extract radar_name, strategy, vol_nr, and field_type from a BUFR filename.

    Uses pre-compiled regex for efficient repeated calls.

    BUFR filename format: RADAR_VOLCODE_VOLNR_FIELD_TIMESTAMP.BUFR
    Example: RMA11_0302_01_TH_20251120T120000Z.BUFR

    Args:
        filename: BUFR filename to parse

    Returns:
        Dictionary with keys: radar_name, vol_code, vol_nr, field_type, timestamp
        Returns None for any key if extraction fails.

    Example:
        >>> result = extract_bufr_filename_components('RMA11_0302_01_TH_20251120T120000Z.BUFR')
        >>> result
        {'radar_name': 'RMA11', 'vol_code': '0302', 'vol_nr': '01', 'field_type': 'TH', 'timestamp': '20251120T120000Z'}
    """
    match = config._BUFR_FILENAME_PATTERN.match(filename)

    if match:
        return {
            "radar_name": match.group(1),
            "strategy": match.group(2),
            "vol_nr": match.group(3),
            "field_type": match.group(4),
            "timestamp": match.group(5),
        }
    else:
        return {
            "radar_name": None,
            "strategy": None,
            "vol_nr": None,
            "field_type": None,
            "timestamp": None,
        }


def build_vol_types_regex(vol_types: Dict[str, Dict[str, list]]) -> Optional[re.Pattern]:
    """
    Build a compiled regex pattern from vol_types dictionary to match BUFR filenames.

    The vol_types dictionary structure:
        vol_types['vol_code'] = {'vol_nr': ['FIELD1', 'FIELD2', ...], ...}

    BUFR filename format (assumed): RADAR_VOLCODE_VOLNR_FIELD_TIMESTAMP.BUFR

    The regex will match filenames where vol_code, vol_nr, and field are all present
    in the vol_types dictionary.

    Example:
        >>> vol_types = {
        ...     '0302': {'01': ['TH', 'TV', 'DBZH']},
        ...     '0303': {'01': ['RHOHV', 'KDP'], '02': ['VRAD']}
        ... }
        >>> regex = build_vol_types_regex(vol_types)
        >>> regex.match('RMA11_0302_01_TH_20251120T120000Z.BUFR')  # True
        >>> regex.match('RMA11_0302_01_ZDR_20251120T120000Z.BUFR')  # False (ZDR not in list)

    Args:
        vol_types: Dictionary mapping vol_code -> {vol_nr -> [field_names]}

    Returns:
        Compiled regex pattern, or None if vol_types is empty.
    """
    if not vol_types:
        return None

    # Build list of patterns: vol_code_vol_nr_field combinations
    patterns = []

    for vol_code, vol_numbers in vol_types.items():
        for vol_nr, fields in vol_numbers.items():
            for field in fields:
                # Create pattern: _VOLCODE_VOLNR_FIELD_
                # Using escaped characters to handle special regex chars
                pattern = f"_{re.escape(vol_code)}_{re.escape(vol_nr)}_{re.escape(field)}_"
                patterns.append(pattern)

    if not patterns:
        return None

    # Combine all patterns with OR (|)
    combined_pattern = "|".join(patterns)

    # Add anchors: match anywhere in filename and end with .BUFR
    full_pattern = f"^.*(?:{combined_pattern}).*\\.BUFR$"

    try:
        return re.compile(full_pattern, re.IGNORECASE)
    except re.error as e:
        logger.error("Failed to compile vol_types regex: %s", e)
        return None
