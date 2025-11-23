"""
Custom colormap definitions and automatic registration for radarlib.

This module defines custom colormaps for radar visualization and automatically
registers them with matplotlib when imported. Each colormap is registered with
both normal and reversed (_r) versions using the 'grc_' prefix.
"""

import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap, ListedColormap

# =============================================================================
# Colormap Specifications
# =============================================================================

# Velocity colormap - suitable for radial velocity fields
_vrad = {
    "red": [(0.0, 0, 0), (0.45, 0, 0), (0.5, 0.05, 0.05), (0.55, 0.5, 0.5), (1, 1, 1)],
    "green": [(0.0, 1, 1), (0.45, 0.5, 0.5), (0.5, 0.05, 0.05), (0.55, 0, 0), (1, 0, 0)],
    "blue": [(0.0, 0, 0), (0.45, 0, 0), (0.5, 0.05, 0.05), (0.55, 0, 0), (1, 0, 0)],
}
_rho = {
    "red": [
        (0.0, 0.38, 0.38),
        (0.3, 0.16, 0.16),
        (0.5, 0.27, 0.27),
        (0.7, 0.31, 0.31),
        (0.8, 0.49, 0.49),
        (0.85, 0, 0),
        (0.9, 1, 1),
        (0.95, 1, 1),
        (0.98, 0.67, 0.67),
        (0.995, 0.67, 0.67),
        (1, 1, 1),
    ],
    "green": [
        (0.0, 0, 0),
        (0.3, 0.28, 0.28),
        (0.5, 0.55, 0.55),
        (0.7, 0.78, 0.78),
        (0.8, 0.89, 0.89),
        (0.85, 0.81, 0.81),
        (0.9, 1, 1),
        (0.95, 0.55, 0.55),
        (0.98, 0, 0),
        (0.995, 0, 0),
        (1, 0, 0),
    ],
    "blue": [
        (0.0, 0.57, 0.57),
        (0.3, 0.97, 0.97),
        (0.5, 0.83, 0.83),
        (0.7, 1, 1),
        (0.8, 1, 1),
        (0.85, 0, 0),
        (0.9, 0, 0),
        (0.95, 0.45, 0.45),
        (0.98, 0, 0),
        (0.995, 0, 0),
        (1, 1, 1),
    ],
}

_th = {
    "red": [
        (0.0, 1, 1),
        (0.33, 0.95, 0.95),
        (0.4, 0.24, 0.24),
        (0.45, 0.22, 0.22),
        (0.55, 0.04, 0.04),
        (0.63, 0.95, 0.95),
        (0.85, 1, 1),
        (1, 1, 1),
    ],
    "green": [
        (0.0, 1, 1),
        (0.33, 0.97, 0.97),
        (0.4, 0.46, 0.46),
        (0.4, 0.98, 0.98),
        (0.55, 0.62, 0.62),
        (0.63, 1, 1),
        (0.85, 0, 0),
        (1, 0, 0),
    ],
    "blue": [
        (0.0, 1, 1),
        (0.33, 0.95, 0.95),
        (0.4, 0.78, 0.78),
        (0.4, 0.52, 0.52),
        (0.55, 0.27, 0.27),
        (0.63, 0, 0),
        (0.85, 0, 0),
        (1, 1, 1),
    ],
}

_th1 = {
    "red": [
        (0.0, 1, 1),
        (0.2, 0.95, 0.95),
        (0.4, 0.24, 0.24),
        (0.45, 0.22, 0.22),
        (0.55, 0.04, 0.04),
        (0.63, 0.95, 0.95),
        (0.85, 1, 1),
        (1, 1, 1),
    ],
    "green": [
        (0.0, 1, 1),
        (0.2, 0.97, 0.97),
        (0.4, 0.46, 0.46),
        (0.4, 0.98, 0.98),
        (0.55, 0.62, 0.62),
        (0.63, 1, 1),
        (0.85, 0, 0),
        (1, 0, 0),
    ],
    "blue": [
        (0.0, 1, 1),
        (0.2, 0.95, 0.95),
        (0.4, 0.78, 0.78),
        (0.4, 0.52, 0.52),
        (0.55, 0.27, 0.27),
        (0.63, 0, 0),
        (0.85, 0, 0),
        (1, 1, 1),
    ],
}

_th2 = {
    "red": [
        (0.0, 1, 1),
        (0.25, 0.95, 0.95),
        (0.5, 0.24, 0.24),
        (0.5, 0.22, 0.22),
        (0.55, 0.04, 0.04),
        (0.63, 0.95, 0.95),
        (0.85, 1, 1),
        (1, 1, 1),
    ],
    "green": [
        (0.0, 1, 1),
        (0.25, 0.97, 0.97),
        (0.5, 0.46, 0.46),
        (0.5, 0.98, 0.98),
        (0.55, 0.62, 0.62),
        (0.63, 1, 1),
        (0.85, 0, 0),
        (1, 0, 0),
    ],
    "blue": [
        (0.0, 1, 1),
        (0.25, 0.95, 0.95),
        (0.5, 0.78, 0.78),
        (0.5, 0.52, 0.52),
        (0.55, 0.27, 0.27),
        (0.63, 0, 0),
        (0.85, 0, 0),
        (1, 1, 1),
    ],
}
# Dictionary to store all colormap specifications
# Add more colormap specifications here as needed
datad = {
    "vrad": _vrad,
    "rho": _rho,
    "th": _th,
    "th1": _th1,
    "th2": _th2,
}

# Discrete cmaps
# =============================================================================
# Discretas
# =============================================================================
_zdr = ["#b7b7b7", "#0055FF", "#66b3df", "#00FFFF", "#489D39", "#F9EA3C", "#FF078B"]


_hscan = [
    "#adccff",
    "#3f88ff",
    "#0060ff",
    "#ffd77c",
    "#ffaf00",
    "#ff601c",
    "#ff6363",
    "#ff4242",
    "#fc2f2f",
    "#ff1c1c",
    "#ff0000",
    "#f4a5ff",
    "#e168f2",
    "#e635ff",
    "#bd00d8",
    "#7c008e",
    "#b2b2b2",
    "#898989",
    "#5b5b5b",
    "#262525",
]


_hid_flCSU = [
    "White",
    "LightBlue",
    "MediumBlue",
    "DarkOrange",
    "LightPink",
    "Cyan",
    "DarkGray",
    "Lime",
    "Yellow",
    "Red",
    "Fuchsia",
]

_hid_ss = ["White", "LightBlue", "MediumBlue", "DarkOrange", "LightPink", "Cyan", "DarkGray", "Lime", "Yellow", "Red"]

data_listedcmap = {
    "zdr": _zdr,
    "hscan": _hscan,
    "hid_flCSU": _hid_flCSU,
    "hid_ss": _hid_ss,
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

    for name, cmap in data_listedcmap.items():
        full_name = "grc_" + name
        try:
            cmap = ListedColormap(cmap, full_name)
            plt.colormaps.register(cmap=cmap, name=full_name, force=False)
            cmap_d[name] = cmap
        except ValueError:
            # Colormap already registered, skip
            pass

    return cmap_d


# # registra los colormaps discretos
# for name, cmap in data_listedcmap.items():
#     full_name = 'grc_' + name
#     cmap = mpl.colors.ListedColormap(cmap, full_name)
#     cm.register_cmap(cmap=cmap)

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
