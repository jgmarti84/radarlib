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
}

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


def reload(path: Optional[str] = None) -> None:
    """Force reload configuration. If path is provided, try it first."""
    global _config
    _config = DEFAULTS.copy()
    if path:
        _try_load_file(path)
    _auto_load()
