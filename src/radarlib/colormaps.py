"""
Custom colormap definitions and automatic registration for radarlib.

This module defines custom colormaps for radar visualization and automatically
registers them with matplotlib when imported. Each colormap is registered with
both normal and reversed (_r) versions using the 'grc_' prefix.
"""

import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap

# =============================================================================
# Colormap Specifications
# =============================================================================

# Velocity colormap - suitable for radial velocity fields
_vrad = {
    "red": [(0.0, 0, 0), (0.45, 0, 0), (0.5, 0.05, 0.05), (0.55, 0.5, 0.5), (1, 1, 1)],
    "green": [(0.0, 1, 1), (0.45, 0.5, 0.5), (0.5, 0.05, 0.05), (0.55, 0, 0), (1, 0, 0)],
    "blue": [(0.0, 0, 0), (0.45, 0, 0), (0.5, 0.05, 0.05), (0.55, 0, 0), (1, 0, 0)],
}

# Dictionary to store all colormap specifications
# Add more colormap specifications here as needed
datad = {
    "vrad": _vrad,
}


# =============================================================================
# Colormap Generation and Registration
# =============================================================================


def _reverse_cmap_spec(spec):
    """
    Reverse a colormap specification.

    Parameters
    ----------
    spec : dict
        Colormap specification with 'red', 'green', 'blue' keys

    Returns
    -------
    dict
        Reversed colormap specification
    """
    reversed_spec = {}
    for key in ["red", "green", "blue"]:
        if key in spec:
            # Reverse the list and invert the x-coordinates (position values)
            reversed_list = []
            for x, y0, y1 in reversed(spec[key]):
                reversed_list.append((1.0 - x, y0, y1))
            reversed_spec[key] = reversed_list
    return reversed_spec


def _generate_cmap(name, lut_size):
    """
    Generate a LinearSegmentedColormap from a specification.

    Parameters
    ----------
    name : str
        Name of the colormap (must exist in datad)
    lut_size : int
        Number of entries in the lookup table

    Returns
    -------
    LinearSegmentedColormap
        Generated colormap object
    """
    spec = datad[name]
    return LinearSegmentedColormap(name, spec, lut_size)


def init_cmaps():
    """
    Initialize and register all custom colormaps with matplotlib.

    This function:
    1. Gets the LUT size from matplotlib configuration
    2. Generates reversed specifications for all colormaps
    3. Creates colormap objects for both normal and reversed versions
    4. Registers all colormaps with matplotlib using 'grc_' prefix

    The colormaps are registered in matplotlib's colormap registry and
    can be accessed using their names (e.g., 'grc_vrad', 'grc_vrad_r').
    """
    LUTSIZE = mpl.rcParams["image.lut"]

    # Store generated colormaps
    cmap_d = {}

    # Need this list because datad is changed in loop
    _cmapnames = list(datad.keys())

    # Generate the reversed specifications
    for cmapname in _cmapnames:
        spec = datad[cmapname]
        spec_reversed = _reverse_cmap_spec(spec)
        datad[cmapname + "_r"] = spec_reversed

    # Precache the cmaps with lutsize = LUTSIZE
    # Use datad.keys() to also add the reversed ones added in the section above
    for cmapname in datad.keys():
        cmap_d[cmapname] = _generate_cmap(cmapname, LUTSIZE)

    # Register the colormaps with matplotlib
    for name, cmap in cmap_d.items():
        full_name = "grc_" + name
        try:
            plt.colormaps.register(cmap=cmap, name=full_name, force=False)
        except ValueError:
            # Colormap already registered, skip
            pass

    return cmap_d


# =============================================================================
# Automatic Initialization
# =============================================================================

# Automatically initialize and register colormaps when module is imported
_registered_colormaps = init_cmaps()

# Export the registered colormap names for reference
REGISTERED_COLORMAP_NAMES = ["grc_" + name for name in _registered_colormaps.keys()]

__all__ = [
    "datad",
    "init_cmaps",
    "_reverse_cmap_spec",
    "_generate_cmap",
    "REGISTERED_COLORMAP_NAMES",
]
