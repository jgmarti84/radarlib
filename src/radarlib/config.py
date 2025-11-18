"""Simple configuration loader for radarlib.

Load order:
1. File pointed to by RADARLIB_CONFIG env var (if set).
2. <package>/radarlib.json (if present).
3. Built-in defaults.

Config file format: JSON dictionary, e.g. {"MAX_SAMPLES": 100, "DEFAULT_TIMEOUT": 5.0}
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

root_proyect = os.path.dirname(os.path.abspath(__file__))

DEFAULTS: Dict[str, Any] = {
    "BUFR_RESOURCES_PATH": os.path.join(root_proyect, "io", "bufr", "bufr_resources"),
    "ROOT_CACHE_PATH": os.path.join(root_proyect, "cache"),
    "ROOT_RADAR_FILES_PATH": os.path.join(root_proyect, "radares"),
    "COLMAX_ELEV_LIMIT1": 0.65,
    "COLMAX_RHOHV_FILTER": True,
    "COLMAX_RHOHV_UMBRAL": 0.8,
    "COLMAX_WRAD_FILTER": True,
    "COLMAX_WRAD_UMBRAL": 4.6,
    "COLMAX_TDR_FILTER": True,
    "COLMAX_TDR_UMBRAL": 8.5,
    "FTP_HOST": "www.example.com",
    "FTP_USER": "example_user",
    "FTP_PASS": "secret",
    "VMIN_REFL_NOFILTERS": -20,
    "VMAX_REFL_NOFILTERS": 70,
    "CMAP_REFL_NOFILTERS": "grc_th",
    "VMIN_RHOHV_NOFILTERS": 0,
    "VMAX_RHOHV_NOFILTERS": 1,
    "CMAP_RHOHV_NOFILTERS": "grc_rho",
    "VMIN_PHIDP_NOFILTERS": -5,
    "VMAX_PHIDP_NOFILTERS": 360,
    "CMAP_PHIDP_NOFILTERS": "grc_th",
    "VMIN_KDP_NOFILTERS": -4,
    "VMAX_KDP_NOFILTERS": 8,
    "CMAP_KDP_NOFILTERS": "jet",
    "VMIN_ZDR_NOFILTERS": -7.5,
    "VMAX_ZDR_NOFILTERS": 7.5,
    "CMAP_ZDR_NOFILTERS": "grc_zdr",
    "VMIN_VRAD_NOFILTERS": -30,
    "VMAX_VRAD_NOFILTERS": 30,
    "CMAP_VRAD_NOFILTERS": "grc_vrad",
    "VMIN_WRAD_NOFILTERS": -2,
    "VMAX_WRAD_NOFILTERS": 6,
    "CMAP_WRAD_NOFILTERS": "grc_th",
    "VMIN_REFL": -20,
    "VMAX_REFL": 70,
    "CMAP_REFL": "grc_th",
    "VMIN_RHOHV": 0,
    "VMAX_RHOHV": 1,
    "CMAP_RHOHV": "grc_rho",
    "VMIN_PHIDP": -5,
    "VMAX_PHIDP": 360,
    "CMAP_PHIDP": "grc_th",
    "VMIN_KDP": -4,
    "VMAX_KDP": 8,
    "CMAP_KDP": "jet",
    "VMIN_ZDR": -2,
    "VMAX_ZDR": 7.5,
    "CMAP_ZDR": "grc_zdr",
    "VMIN_VRAD": -15,
    "VMAX_VRAD": 15,
    "CMAP_VRAD": "grc_vrad",
    "VMIN_WRAD": -2,
    "VMAX_WRAD": 6,
    "CMAP_WRAD": "grc_th",
    # dpi = 72
}

# # Paraémtros para Filtros
# colmax_threshold = -3
# grc_filter = {
#     'rhv_filter1': True, 'rhv_threshold1': 0.55,
#     'wrad_filter': True, 'wrad_threshold': 4.6,
#     'refl_filter': True, 'refl_threshold': -3,
#     'zdr_filter': True, 'zdr_threshold': 8.5,
#     'refl_filter2': True, 'refl_threshold2': 25,
#     'cm_filter': True,
#     'rhohv_threshold2': 0.85, 'despeckle_filter': True,
#     'mean_filter': True, 'mean_threshold': 0.85,
# }

_config: Dict[str, Any] = DEFAULTS.copy()


def _try_load_file(path: str) -> bool:
    try:
        with open(path, "r", encoding="utf8") as fh:
            data = json.load(fh)
        if isinstance(data, dict):
            _config.update(data)
            return True
    except FileNotFoundError:
        return False
    except Exception:
        # ignore parse errors / other I/O errors to keep loader robust
        return False
    return False


def _auto_load() -> None:
    # 1) explicit env var
    env_path = os.environ.get("RADARLIB_CONFIG")
    if env_path and _try_load_file(env_path):
        return

    # Override with environment variables if they exist
    for key in _config.keys():
        env_val = os.environ.get(key)
        if env_val is not None:
            # Try to preserve type from defaults
            default_val = DEFAULTS.get(key)
            if isinstance(default_val, bool):
                _config[key] = env_val.lower() in ("true", "1", "yes")
            elif isinstance(default_val, (int, float)):
                try:
                    _config[key] = type(default_val)(env_val)
                except ValueError:
                    pass
            else:
                _config[key] = env_val

    # # 2) package-local radarlib.json (src/radarlib/radarlib.json)
    # pkg_local = Path(__file__).resolve().parent / "radarlib.json"
    # if _try_load_file(str(pkg_local)):
    #     return

    # # 3) project root candidate (one level up)
    # project_local = Path(__file__).resolve().parent.parent / "radarlib.json"
    # _try_load_file(str(project_local))


_auto_load()


def get(key: str, default: Any = None) -> Any:
    return _config.get(key, default)


# convenience attributes
BUFR_RESOURCES_PATH: str = get("BUFR_RESOURCES_PATH")
ROOT_CACHE_PATH: str = get("ROOT_CACHE_PATH")
ROOT_RADAR_FILES_PATH: str = get("ROOT_RADAR_FILES_PATH")
COLMAX_ELEV_LIMIT1: float = get("COLMAX_ELEV_LIMIT1")
COLMAX_RHOHV_FILTER: bool = get("COLMAX_RHOHV_FILTER")
COLMAX_RHOHV_UMBRAL: float = get("COLMAX_RHOHV_UMBRAL")
COLMAX_WRAD_FILTER: bool = get("COLMAX_WRAD_FILTER")
COLMAX_WRAD_UMBRAL: float = get("COLMAX_WRAD_UMBRAL")
COLMAX_TDR_FILTER: bool = get("COLMAX_TDR_FILTER")
COLMAX_TDR_UMBRAL: float = get("COLMAX_TDR_UMBRAL")
FTP_HOST: str = get("FTP_HOST")
FTP_USER: str = get("FTP_USER")
FTP_PASS: str = get("FTP_PASS")
VMIN_REFL_NOFILTERS: int = get("VMIN_REFL_NOFILTERS")
VMAX_REFL_NOFILTERS: int = get("VMAX_REFL_NOFILTERS")
CMAP_REFL_NOFILTERS: str = get("CMAP_REFL_NOFILTERS")
VMIN_RHOHV_NOFILTERS: int = get("VMIN_RHOHV_NOFILTERS")
VMAX_RHOHV_NOFILTERS: int = get("VMAX_RHOHV_NOFILTERS")
CMAP_RHOHV_NOFILTERS: str = get("CMAP_RHOHV_NOFILTERS")
VMIN_PHIDP_NOFILTERS: int = get("VMIN_PHIDP_NOFILTERS")
VMAX_PHIDP_NOFILTERS: int = get("VMAX_PHIDP_NOFILTERS")
CMAP_PHIDP_NOFILTERS: str = get("CMAP_PHIDP_NOFILTERS")
VMIN_KDP_NOFILTERS: int = get("VMIN_KDP_NOFILTERS")
VMAX_KDP_NOFILTERS: int = get("VMAX_KDP_NOFILTERS")
CMAP_KDP_NOFILTERS: str = get("CMAP_KDP_NOFILTERS")
VMIN_ZDR_NOFILTERS: float = get("VMIN_ZDR_NOFILTERS")
VMAX_ZDR_NOFILTERS: float = get("VMAX_ZDR_NOFILTERS")
CMAP_ZDR_NOFILTERS: str = get("CMAP_ZDR_NOFILTERS")
VMIN_VRAD_NOFILTERS: int = get("VMIN_VRAD_NOFILTERS")
VMAX_VRAD_NOFILTERS: int = get("VMAX_VRAD_NOFILTERS")
CMAP_VRAD_NOFILTERS: str = get("CMAP_VRAD_NOFILTERS")
VMIN_WRAD_NOFILTERS: int = get("VMIN_WRAD_NOFILTERS")
VMAX_WRAD_NOFILTERS: int = get("VMAX_WRAD_NOFILTERS")
CMAP_WRAD_NOFILTERS: str = get("CMAP_WRAD_NOFILTERS")
VMIN_REFL: int = get("VMIN_REFL")
VMAX_REFL: int = get("VMAX_REFL")
CMAP_REFL: str = get("CMAP_REFL")
VMIN_RHOHV: int = get("VMIN_RHOHV")
VMAX_RHOHV: int = get("VMAX_RHOHV")
CMAP_RHOHV: str = get("CMAP_RHOHV")
VMIN_PHIDP: int = get("VMIN_PHIDP")
VMAX_PHIDP: int = get("VMAX_PHIDP")
CMAP_PHIDP: str = get("CMAP_PHIDP")
VMIN_KDP: int = get("VMIN_KDP")
VMAX_KDP: int = get("VMAX_KDP")
CMAP_KDP: str = get("CMAP_KDP")
VMIN_ZDR: int = get("VMIN_ZDR")
VMAX_ZDR: float = get("VMAX_ZDR")
CMAP_ZDR: str = get("CMAP_ZDR")
VMIN_VRAD: int = get("VMIN_VRAD")
VMAX_VRAD: int = get("VMAX_VRAD")
CMAP_VRAD: str = get("CMAP_VRAD")
VMIN_WRAD: int = get("VMIN_WRAD")
VMAX_WRAD: int = get("VMAX_WRAD")
CMAP_WRAD: str = get("CMAP_WRAD")


def reload(path: Optional[str] = None) -> None:
    """Force reload configuration. If path is provided, try it first."""
    global _config
    _config = DEFAULTS.copy()
    if path:
        _try_load_file(path)
    _auto_load()
