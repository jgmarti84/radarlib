# This file marks the directory as a Python package and
# can include package-level documentation or initialization code.
import os

from pyart.config import load_config

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
