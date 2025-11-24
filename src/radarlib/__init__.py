# This file marks the directory as a Python package and
# can include package-level documentation or initialization code.
import os

from pyart.config import load_config

# Import config for easier access
# Import colormaps module to automatically register custom colormaps
from radarlib import colormaps  # noqa: F401
from radarlib import config  # noqa: F401

# Note: daemons and state modules are available but not imported at startup
# to avoid circular imports. Import them directly when needed:
#   from radarlib.daemons import DownloadDaemon, ProcessingDaemon, ProductGenerationDaemon
#   from radarlib.state import SQLiteStateTracker, FileStateTracker

_citation_text = """
### ----------------------------------------------------------------------- ###
###                         Grupo Radar Cordoba                             ###
### ----------------------------------------------------------------------- ###
"""

if "PYART_QUIET" not in os.environ:
    print(_citation_text)

_dirname = os.path.dirname(__file__)
global_parameters = os.path.join(_dirname, "pyart_defaults.py")
load_config(filename=global_parameters)
