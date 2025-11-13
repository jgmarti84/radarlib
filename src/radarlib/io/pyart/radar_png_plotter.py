"""
PyART radar visualization and PNG generation utilities.

This module provides clean, modular functions for generating PNG visualizations
of PyART radar objects using PPI (Plan Position Indicator) plots.
"""

import logging
import os
from typing import Dict, List, Optional, Tuple

import matplotlib.axes
import matplotlib.figure
import matplotlib.pyplot as plt
import pyart
from pyart.config import get_field_colormap
from pyart.core import Radar

logger = logging.getLogger(__name__)


class RadarPlotConfig:
    """Configuration for radar PPI plot styling and output."""

    def __init__(
        self,
        figsize: Tuple[int, int] = (12, 12),
        dpi: int = 150,
        transparent: bool = True,
        colorbar: bool = False,
        title: bool = False,
        axis_labels: bool = False,
        tight_layout: bool = True,
    ):
        """
        Initialize plot configuration.

        Parameters
        ----------
        figsize : tuple
            Figure size as (width, height)
        dpi : int
            Resolution in dots per inch
        transparent : bool
            Whether to use transparent background
        colorbar : bool
            Whether to display colorbar
        title : bool
            Whether to display title
        axis_labels : bool
            Whether to display axis labels
        tight_layout : bool
            Whether to apply tight layout
        """
        self.figsize = figsize
        self.dpi = dpi
        self.transparent = transparent
        self.colorbar = colorbar
        self.title = title
        self.axis_labels = axis_labels
        self.tight_layout = tight_layout


class FieldPlotConfig:
    """Configuration for individual field plot styling."""

    def __init__(
        self,
        field_name: str,
        vmin: Optional[float] = None,
        vmax: Optional[float] = None,
        cmap: Optional[str] = None,
        sweep: Optional[int] = None,
    ):
        """
        Initialize field plot configuration.

        Parameters
        ----------
        field_name : str
            Name of the radar field
        vmin : float, optional
            Minimum value for color scale. If None, uses field defaults.
        vmax : float, optional
            Maximum value for color scale. If None, uses field defaults.
        cmap : str, optional
            Colormap name. If None, uses field defaults.
        sweep : int, optional
            Specific sweep index to plot. If None, uses lowest sweep for reflectivity.
        """
        self.field_name = field_name
        self.sweep = sweep

        # Use field-specific defaults if not provided
        if vmin is None or vmax is None or cmap is None:
            field_defaults = self._get_field_defaults(field_name)
            self.vmin = vmin if vmin is not None else field_defaults["vmin"]
            self.vmax = vmax if vmax is not None else field_defaults["vmax"]
            self.cmap = cmap if cmap is not None else field_defaults["cmap"]
            if self.sweep is None:
                self.sweep = field_defaults.get("sweep")
        else:
            self.vmin = vmin
            self.vmax = vmax
            self.cmap = cmap

    @staticmethod
    def _get_field_defaults(field_name: str) -> Dict[str, any]:  # type: ignore
        """
        Get default configuration values for specific radar fields.

        Parameters
        ----------
        field_name : str
            Name of the radar field

        Returns
        -------
        dict
            Dictionary with 'vmin', 'vmax', 'cmap', and optionally 'sweep' keys
        """
        field_configs = {
            "TH": {
                "vmin": -20,
                "vmax": 70,
                "cmap": get_field_colormap("TH"),
                # sweep: will use get_lowest_nsweep (handled in calling code)
            },
            "TV": {
                "vmin": -20,
                "vmax": 70,
                "cmap": get_field_colormap("TV"),
            },
            "DBZH": {
                "vmin": -20,
                "vmax": 70,
                "cmap": get_field_colormap("DBZH"),
            },
            "DBZV": {
                "vmin": -20,
                "vmax": 70,
                "cmap": get_field_colormap("DBZV"),
            },
            "COLMAX": {
                "vmin": -20,
                "vmax": 70,
                "cmap": get_field_colormap("COLMAX"),
                "sweep": 0,
            },
            "RHOHV": {
                "vmin": 0,
                "vmax": 1,
                "cmap": get_field_colormap("RHOHV"),
                "sweep": 0,
            },
            "PHIDP": {
                "vmin": -5,
                "vmax": 360,
                "cmap": get_field_colormap("PHIDP"),
                "sweep": 0,
            },
            "KDP": {
                "vmin": -4,
                "vmax": 8,
                "cmap": get_field_colormap("KDP"),
                "sweep": 0,
            },
            "ZDR": {
                "vmin": -2,
                "vmax": 7.5,
                "cmap": get_field_colormap("ZDR"),
                "sweep": 0,
            },
            "TDR": {
                "vmin": -2,
                "vmax": 7.5,
                "cmap": get_field_colormap("TDR"),
                "sweep": 0,
            },
            "VRAD": {
                "vmin": -15,
                "vmax": 15,
                "cmap": get_field_colormap("VRAD"),
                "sweep": 0,
            },
            "WRAD": {
                "vmin": -2,
                "vmax": 6,
                "cmap": get_field_colormap("WRAD"),
                "sweep": 0,
            },
        }

        # Return field-specific config or sensible defaults
        if field_name in field_configs:
            return field_configs[field_name]
        else:
            return {
                "vmin": -20,
                "vmax": 80,
                "cmap": "viridis",
            }


def setup_plot_figure(config: RadarPlotConfig) -> Tuple[matplotlib.figure.Figure, matplotlib.axes.Axes]:
    """
    Set up a matplotlib figure with proper styling for radar plots.

    Parameters
    ----------
    config : RadarPlotConfig
        Plot configuration object

    Returns
    -------
    fig : matplotlib.figure.Figure
        The figure object
    ax : matplotlib.axes.Axes
        The axes object
    """
    plt.ioff()  # Disable interactive mode

    fig = plt.figure(figsize=config.figsize)
    ax = fig.add_subplot(1, 1, 1)

    # Remove frame and spines
    for spine in ax.spines.values():
        spine.set_visible(False)

    plt.axis("off")
    ax.set_aspect("equal")

    if config.tight_layout:
        plt.tight_layout(pad=0)
        plt.subplots_adjust(top=1, bottom=0, right=1, left=0, hspace=0, wspace=0)
        plt.margins(0, 0)

    return fig, ax


def plot_ppi_field(
    radar: Radar,
    field: str,
    sweep: Optional[int] = None,
    config: Optional[RadarPlotConfig] = None,
    field_config: Optional[FieldPlotConfig] = None,
) -> Tuple[matplotlib.figure.Figure, matplotlib.axes.Axes]:
    """
    Create a PPI plot for a single field of a radar object.

    Automatically applies field-specific defaults for vmin, vmax, cmap, and sweep.

    Parameters
    ----------
    radar : Radar
        PyART radar object
    field : str
        Name of the field to plot
    sweep : int, optional
        Sweep index to plot. If None, uses field-specific default (0 for most fields).
        For 'TH' and 'TV' fields, defaults to lowest sweep with data.
    config : RadarPlotConfig, optional
        Plot configuration. If None, uses defaults.
    field_config : FieldPlotConfig, optional
        Field plot configuration. If None, automatically creates one with field defaults.

    Returns
    -------
    fig : matplotlib.figure.Figure
        The figure object
    ax : matplotlib.axes.Axes
        The axes object

    Raises
    ------
    ValueError
        If field is not in radar or sweep index is invalid
    """
    if config is None:
        config = RadarPlotConfig()

    if field not in radar.fields:
        raise ValueError(f"Field '{field}' not found in radar. Available fields: {list(radar.fields.keys())}")

    # Create field configuration with automatic defaults if not provided
    if field_config is None:
        field_config = FieldPlotConfig(field)

    # Determine sweep to use
    if sweep is None:
        sweep = field_config.sweep if field_config.sweep is not None else 0

    if sweep >= radar.nsweeps:
        raise ValueError(f"Sweep {sweep} out of range. Radar has {radar.nsweeps} sweeps.")

    # Set up figure
    fig, ax = setup_plot_figure(config)

    # Create radar display
    display = pyart.graph.RadarDisplay(radar)

    # Plot PPI with field configuration
    display.plot_ppi(
        field=field,
        sweep=sweep,
        vmin=field_config.vmin,
        vmax=field_config.vmax,
        cmap=field_config.cmap,
        mask_outside=False,
        title=None if not config.title else field,
        title_flag=config.title,
        axislabels=(None, None) if not config.axis_labels else ("X (km)", "Y (km)"),
        axislabels_flag=config.axis_labels,
        colorbar_flag=config.colorbar,
        edges=True,
        filter_transitions=True,
    )

    return fig, ax


def save_ppi_png(
    fig: matplotlib.figure.Figure,
    output_path: str,
    filename: str,
    dpi: int = 150,
    transparent: bool = True,
) -> str:
    """
    Save a PPI figure to a PNG file.

    Parameters
    ----------
    fig : matplotlib.figure.Figure
        Figure to save
    output_path : str
        Directory where to save the file
    filename : str
        Name of the output file
    dpi : int
        Resolution in dots per inch
    transparent : bool
        Whether to use transparent background

    Returns
    -------
    str
        Full path to saved file
    """
    # Create directory if it doesn't exist
    os.makedirs(output_path, exist_ok=True)

    full_path = os.path.join(output_path, filename)
    fig.savefig(full_path, dpi=dpi, transparent=transparent)

    logger.info(f"Saved: {full_path}")
    return full_path


def plot_and_save_ppi(
    radar: Radar,
    field: str,
    output_path: str,
    filename: str,
    sweep: Optional[int] = None,
    config: Optional[RadarPlotConfig] = None,
    field_config: Optional[FieldPlotConfig] = None,
) -> str:
    """
    Create a PPI plot for a field and save it as PNG.

    This is a convenience function that combines plotting and saving.
    Automatically applies field-specific defaults if not provided.

    Parameters
    ----------
    radar : Radar
        PyART radar object
    field : str
        Name of the field to plot
    output_path : str
        Directory where to save the PNG
    filename : str
        Name of the output file (should end with .png)
    sweep : int, optional
        Sweep index to plot. If None, uses field-specific default.
    config : RadarPlotConfig, optional
        Plot configuration. If None, uses defaults.
    field_config : FieldPlotConfig, optional
        Field plot configuration. If None, automatically creates one with field defaults.

    Returns
    -------
    str
        Full path to saved file

    Examples
    --------
    >>> # Use automatic field defaults
    >>> plot_and_save_ppi(radar, 'RHOHV', 'output/', 'rhohv_sweep0.png')

    >>> # Override field defaults
    >>> custom_config = FieldPlotConfig('TH', vmin=-10, vmax=60)
    >>> plot_and_save_ppi(radar, 'TH', 'output/', 'th_custom.png', field_config=custom_config)
    """
    fig, ax = plot_ppi_field(radar, field, sweep=sweep, config=config, field_config=field_config)

    full_path = save_ppi_png(
        fig,
        output_path,
        filename,
        dpi=config.dpi if config else 150,
        transparent=config.transparent if config else True,
    )

    plt.close(fig)
    return full_path


def plot_multiple_fields(
    radar: Radar,
    fields: List[str],
    output_base_path: str,
    sweep: Optional[int] = None,
    config: Optional[RadarPlotConfig] = None,
    field_configs: Optional[Dict[str, FieldPlotConfig]] = None,
) -> Dict[str, str]:
    """
    Create and save PPI plots for multiple fields.

    Automatically applies field-specific defaults for each field unless overridden.

    Parameters
    ----------
    radar : Radar
        PyART radar object
    fields : list of str
        List of field names to plot
    output_base_path : str
        Base directory for output files
    sweep : int, optional
        Sweep index to plot. If None, uses field-specific defaults.
        Can be overridden per-field using field_configs.
    config : RadarPlotConfig, optional
        Plot configuration for all fields. If None, uses defaults.
    field_configs : dict, optional
        Per-field plot configurations keyed by field name.
        For fields not in this dict, automatic defaults are used.

    Returns
    -------
    results : dict
        Dictionary mapping field name to output file path for successful plots.
        Fields that failed or were not in radar are excluded.

    Examples
    --------
    >>> # Plot with automatic field defaults
    >>> results = plot_multiple_fields(radar, ['RHOHV', 'KDP', 'VRAD'], 'output/')
    >>> print(f"Created {len(results)} PNG files")

    >>> # Override specific field configurations
    >>> custom_configs = {
    ...     'TH': FieldPlotConfig('TH', vmin=-10, vmax=60),
    ...     'VRAD': FieldPlotConfig('VRAD', vmin=-20, vmax=20)
    ... }
    >>> results = plot_multiple_fields(
    ...     radar,
    ...     ['TH', 'VRAD', 'KDP'],
    ...     'output/',
    ...     field_configs=custom_configs
    ... )
    """
    if config is None:
        config = RadarPlotConfig()

    if field_configs is None:
        field_configs = {}

    results = {}

    for field in fields:
        if field not in radar.fields:
            logger.warning(f"Field '{field}' not in radar. Skipping.")
            continue

        try:
            # Get field-specific config if available, otherwise auto-create with defaults
            field_config = field_configs.get(field) or FieldPlotConfig(field)

            # Determine sweep for this field
            field_sweep = sweep if sweep is not None else field_config.sweep
            if field_sweep is None:
                field_sweep = 0

            # Create output filename with sweep number
            filename = f"{field}_sweep{field_sweep:02d}.png"

            # Plot and save
            output_path = plot_and_save_ppi(
                radar,
                field,
                output_base_path,
                filename,
                sweep=field_sweep,
                config=config,
                field_config=field_config,
            )

            results[field] = output_path
            logger.info(f"Successfully plotted field: {field} (sweep {field_sweep})")

        except Exception as e:
            logger.error(f"Error plotting field '{field}': {e}")
            continue

    return results
