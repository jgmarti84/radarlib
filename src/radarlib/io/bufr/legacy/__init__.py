"""
Legacy BUFR to PyART conversion code.

This module contains the original/legacy implementation of BUFR to PyART conversion.
It's kept here for comparison and regression testing purposes against the new
modular implementation in pyart_writer.py.
"""

from .bufr_legacy import bufr_to_dict, dec_bufr_file
from .pyart_legacy import bufr_to_pyart_legacy, read_xml_estrategia2

__all__ = [
    "dec_bufr_file",
    "bufr_to_dict",
    "bufr_to_pyart_legacy",
    "read_xml_estrategia2",
]
