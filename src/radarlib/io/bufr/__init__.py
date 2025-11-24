"""
BUFR module for decoding and converting BUFR radar data.

This module provides:
- `bufr_to_dict`: Decode BUFR files to Python dictionaries
- `bufr_to_pyart`: Convert BUFR data to PyART radar format
- `bufr_fields_to_pyart_radar`: Create PyART radar from BUFR field data
"""

# Backward compatibility - also import from old module name
from radarlib.io.bufr import pyart_writer  # noqa: F401
from radarlib.io.bufr.bufr import bufr_to_dict

# New module name
from radarlib.io.bufr.bufr_to_pyart import bufr_fields_to_pyart_radar, bufr_to_pyart

__all__ = ["bufr_to_dict", "bufr_to_pyart", "bufr_fields_to_pyart_radar"]
