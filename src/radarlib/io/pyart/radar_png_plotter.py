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
from pyart.config import get_field_colormap, get_field_limits
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
        gatefilter: Optional[pyart.filters.GateFilter] = None,
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

        # Get PyART defaults for the field
        field_limits = get_field_limits(field_name)
        field_cmap = get_field_colormap(field_name)

        # Use provided values or fall back to PyART defaults
        if vmin is not None:
            self.vmin = vmin
        elif field_limits is not None and len(field_limits) >= 2:
            self.vmin = field_limits[0]
        else:
            self.vmin = -20

        if vmax is not None:
            self.vmax = vmax
        elif field_limits is not None and len(field_limits) >= 2:
            self.vmax = field_limits[1]
        else:
            self.vmax = 80

        if cmap is not None:
            self.cmap = cmap
        elif field_cmap is not None:
            self.cmap = field_cmap
        else:
            self.cmap = "viridis"

        self.sweep = sweep if sweep is not None else 0
        self.gatefilter = gatefilter


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
    # field=field, sweep=sweep,
    # mask_tuple=None, vmin=vmin, vmax=vmax,
    # norm=None, cmap=cmap, mask_outside=False, title=None, title_flag=False,
    # axislabels=(None, None), axislabels_flag=False, colorbar_flag=False,
    # colorbar_label=None, colorbar_orient='vertical', edges=True, gatefilter=None,
    # filter_transitions=True, ax=None, fig=None, ticks=None, ticklabs=False)

    #     field=field, sweep=sweep, mask_tuple=None, vmin=vmin, vmax=vmax,
    # norm=None, cmap=cmap, mask_outside=False, title=None, title_flag=False,
    # axislabels=(None, None), axislabels_flag=False, colorbar_flag=False,
    # colorbar_label=None, colorbar_orient='vertical', edges=True,
    # gatefilter=gatefilter, filter_transitions=True, ax=None, fig=None,
    # ticks=None, ticklabs=False)

    display.plot_ppi(
        field=field,
        sweep=sweep,
        mask_tuple=None,
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
        gatefilter=field_config.gatefilter,
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


def export_fields_to_geotiff(
    radar: Radar,
    fields: List[str],
    output_base_path: str,
    sweep: Optional[int] = None,
    crs: str = "EPSG:4326",
) -> dict:
    """
    Export radar fields as georeferenced GeoTIFF files.

    This function saves radar data with full geographic referencing and
    geotransform information for use in GIS applications.

    Parameters
    ----------
    radar : Radar
        PyART radar object
    fields : list of str
        List of field names to export
    output_base_path : str
        Base directory for output GeoTIFF files
    sweep : int, optional
        Sweep index to export. If None, uses 0.
    crs : str, optional
        Coordinate reference system (default: "EPSG:4326" for WGS84)

    Returns
    -------
    results : dict
        Dictionary mapping field name to output file path for successful exports.
        Fields that failed are excluded.

    Examples
    --------
    >>> # Export as GeoTIFF with geographic coordinates
    >>> results = export_fields_to_geotiff(
    ...     radar,
    ...     ['DBZH', 'VRAD', 'KDP'],
    ...     'output_geotiff/',
    ... )
    >>> print(f"Exported {len(results)} fields")

    >>> # Use a different coordinate system
    >>> results = export_fields_to_geotiff(
    ...     radar,
    ...     ['RHOHV', 'ZDR'],
    ...     'output/',
    ...     crs="EPSG:32720"  # UTM Zone 20S
    ... )
    """
    try:
        from radarlib.io.pyart.radar_geotiff_exporter import save_multiple_fields_to_geotiff
    except ImportError:
        raise ImportError(
            "radar_geotiff_exporter module not found. " "Make sure rasterio is installed: pip install rasterio"
        )

    if sweep is None:
        sweep = 0

    results = save_multiple_fields_to_geotiff(
        radar,
        fields,
        output_base_path,
        sweep=sweep,
        crs=crs,
    )

    return results


def plot_fields_with_substitution(
    radar: Radar,
    fields_to_plot: List[str],
    output_base_path: str,
    field_substitutions: Optional[Dict[str, str]] = None,
    sweep: Optional[int] = None,
    config: Optional[RadarPlotConfig] = None,
    field_configs: Optional[Dict[str, FieldPlotConfig]] = None,
) -> Dict[str, str]:
    """
    Plot multiple fields with intelligent field substitution.

    This function handles special cases where a field should be substituted with
    another if it doesn't exist in the radar. For example, substituting 'DBZH' with
    'TH' (reflectivity without filters) if the filtered version isn't available.

    This is useful when working with radar data that may or may not have processed
    (filtered) versions of fields.

    Parameters
    ----------
    radar : Radar
        PyART radar object
    fields_to_plot : list of str
        List of field names to attempt to plot
    output_base_path : str
        Base directory for output files
    field_substitutions : dict, optional
        Mapping of field names to their preferred substitutes.
        Example: {'DBZH': 'TH', 'DBZV': 'TV'}
        If a field in fields_to_plot is not in the radar, its substitute
        (if defined) will be used instead. If neither exists, the field is skipped.
    sweep : int, optional
        Sweep index to plot. If None, uses field-specific defaults.
    config : RadarPlotConfig, optional
        Plot configuration for all fields. If None, uses defaults.
    field_configs : dict, optional
        Per-field plot configurations keyed by field name.

    Returns
    -------
    results : dict
        Dictionary mapping actual_field_name to output file path for successful plots.
        Fields that couldn't be plotted are excluded.

    Examples
    --------
    >>> # Define substitution rules: try to plot DBZH, fall back to TH if not available
    >>> substitutions = {
    ...     'DBZH': 'TH',   # reflectivity with filter -> reflectivity without filter
    ...     'DBZV': 'TV',   # vertical reflectivity with filter -> without filter
    ...     'VRAD': 'VRAD'  # no substitution (velocity stays as-is)
    ... }
    >>> results = plot_fields_with_substitution(
    ...     radar,
    ...     fields_to_plot=['DBZH', 'DBZV', 'VRAD', 'KDP'],
    ...     output_base_path='output/',
    ...     field_substitutions=substitutions
    ... )
    >>> for field, path in results.items():
    ...     print(f"Plotted {field}: {path}")
    """
    if config is None:
        config = RadarPlotConfig()

    if field_configs is None:
        field_configs = {}

    if field_substitutions is None:
        field_substitutions = {}

    results = {}
    skipped_fields = []

    for requested_field in fields_to_plot:
        # Determine which field to actually plot
        field_to_use = requested_field

        # Special case: if requested field doesn't exist, try its substitute
        if requested_field not in radar.fields:
            if requested_field in field_substitutions:
                substitute = field_substitutions[requested_field]
                if substitute in radar.fields:
                    logger.debug(f"Field '{requested_field}' not found. Using substitute '{substitute}'")
                    field_to_use = substitute
                else:
                    logger.warning(
                        f"Field '{requested_field}' not in radar, and substitute "
                        f"'{substitute}' also not found. Skipping."
                    )
                    skipped_fields.append(requested_field)
                    continue
            else:
                logger.warning(f"Field '{requested_field}' not in radar. Skipping.")
                skipped_fields.append(requested_field)
                continue

        try:
            # Get field-specific config if available, otherwise auto-create with defaults
            field_config = field_configs.get(field_to_use) or FieldPlotConfig(field_to_use)

            # Determine sweep for this field
            field_sweep = sweep if sweep is not None else field_config.sweep
            if field_sweep is None:
                from radarlib.utils.fields_utils import get_lowest_nsweep

                field_sweep = get_lowest_nsweep(radar)

            # Create output filename with sweep number
            filename = f"{field_to_use}_sweep{field_sweep:02d}.png"

            # Plot and save
            output_path = plot_and_save_ppi(
                radar,
                field_to_use,
                output_base_path,
                filename,
                sweep=field_sweep,
                config=config,
                field_config=field_config,
            )

            results[field_to_use] = output_path
            logger.info(f"Successfully plotted field: {field_to_use} (sweep {field_sweep})")

        except Exception as e:
            logger.error(f"Error plotting field '{field_to_use}': {e}")
            skipped_fields.append(field_to_use)
            continue

    # Log summary
    if skipped_fields:
        logger.warning(f"Skipped {len(skipped_fields)} fields: {skipped_fields}")

    return results


def plot_fields_with_metadata(
    radar: Radar,
    fields_to_plot: List[str],
    output_base_path: str,
    filename_pattern: Optional[str] = None,
    field_substitutions: Optional[Dict[str, str]] = None,
    sweep: Optional[int] = None,
    config: Optional[RadarPlotConfig] = None,
    field_configs: Optional[Dict[str, FieldPlotConfig]] = None,
) -> Dict[str, str]:
    """
    Plot multiple fields and organize output using metadata from radar filename.

    This function extends plot_fields_with_substitution by organizing output
    files into date-based directory structures based on the radar's metadata.
    This is useful for organizing radar products by observation time.

    Parameters
    ----------
    radar : Radar
        PyART radar object
    fields_to_plot : list of str
        List of field names to plot
    output_base_path : str
        Base directory for output files
    filename_pattern : str, optional
        Pattern for organizing output. Options:
        - 'date_based': organize by YYYY/MM/DD/HH directories (default)
        - 'flat': place all files directly in output_base_path
    field_substitutions : dict, optional
        Mapping of field names to their preferred substitutes
    sweep : int, optional
        Sweep index to plot
    config : RadarPlotConfig, optional
        Plot configuration
    field_configs : dict, optional
        Per-field plot configurations

    Returns
    -------
    results : dict
        Dictionary mapping field_name to output file path

    Examples
    --------
    >>> # Organize plots by date from radar metadata
    >>> results = plot_fields_with_metadata(
    ...     radar,
    ...     ['DBZH', 'VRAD'],
    ...     output_base_path='output/',
    ...     filename_pattern='date_based',
    ...     field_substitutions={'DBZH': 'TH'}
    ... )
    >>> # Output structure: output/2025/11/18/12/DBZH_sweep00.png
    """
    if filename_pattern is None:
        filename_pattern = "date_based"

    # results = {}

    # Extract datetime from radar metadata if available
    output_dir = output_base_path
    if filename_pattern == "date_based":
        try:
            metadata = radar.metadata
            if "datetime" in metadata:
                dt = metadata["datetime"]
                output_dir = os.path.join(
                    output_base_path,
                    dt.strftime("%Y"),
                    dt.strftime("%m"),
                    dt.strftime("%d"),
                    dt.strftime("%H"),
                )
            elif "time_coverage_start" in metadata:
                # Try alternative metadata fields
                import dateutil.parser

                dt = dateutil.parser.isoparse(metadata["time_coverage_start"])
                output_dir = os.path.join(
                    output_base_path,
                    dt.strftime("%Y"),
                    dt.strftime("%m"),
                    dt.strftime("%d"),
                    dt.strftime("%H"),
                )
            logger.debug(f"Organizing output into: {output_dir}")
        except Exception as e:
            logger.warning(f"Could not extract datetime from radar metadata: {e}. Using flat structure.")

    # Plot fields
    return plot_fields_with_substitution(
        radar,
        fields_to_plot,
        output_dir,
        field_substitutions=field_substitutions,
        sweep=sweep,
        config=config,
        field_configs=field_configs,
    )


def export_fields_to_multi_format(
    radar: Radar,
    fields: List[str],
    output_base_path: str,
    formats: Optional[List[str]] = None,
    sweep: Optional[int] = None,
    config: Optional[RadarPlotConfig] = None,
    field_configs: Optional[dict] = None,
) -> dict:
    """
    Export radar fields to multiple formats (PNG, GeoTIFF, NetCDF).

    This is a convenience function for batch exporting to multiple output formats.

    Parameters
    ----------
    radar : Radar
        PyART radar object
    fields : list of str
        List of field names to export
    output_base_path : str
        Base directory for all output files
    formats : list of str, optional
        Output formats: "png", "geotiff", "netcdf". Default: ["png", "geotiff"]
    sweep : int, optional
        Sweep index for PNG and GeoTIFF. If None, uses field defaults.
    config : RadarPlotConfig, optional
        Plot configuration for PNG output
    field_configs : dict, optional
        Per-field plot configurations for PNG output

    Returns
    -------
    results : dict
        Dictionary with format keys containing output file paths

    Examples
    --------
    >>> results = export_fields_to_multi_format(
    ...     radar,
    ...     ['DBZH', 'VRAD'],
    ...     'output/',
    ...     formats=['png', 'geotiff']
    ... )
    >>> print(results)
    {'png': {'DBZH': 'output/png/...', ...}, 'geotiff': {'DBZH': 'output/geotiff/...', ...}}
    """
    if formats is None:
        formats = ["png", "geotiff"]

    results = {}

    for fmt in formats:
        if fmt.lower() == "png":
            output_dir = os.path.join(output_base_path, "png")
            png_results = plot_multiple_fields(
                radar, fields, output_dir, sweep=sweep, config=config, field_configs=field_configs
            )
            results["png"] = png_results
            logger.info(f"Exported {len(png_results)} fields to PNG")

        elif fmt.lower() == "geotiff":
            output_dir = os.path.join(output_base_path, "geotiff")
            geotiff_results = export_fields_to_geotiff(radar, fields, output_dir, sweep=sweep)
            results["geotiff"] = geotiff_results
            logger.info(f"Exported {len(geotiff_results)} fields to GeoTIFF")

        elif fmt.lower() == "netcdf":
            from radarlib.io.pyart.radar_geotiff_exporter import radar_to_netcdf_with_coordinates

            output_dir = os.path.join(output_base_path, "netcdf")
            os.makedirs(output_dir, exist_ok=True)
            nc_file = radar_to_netcdf_with_coordinates(radar, output_dir)
            results["netcdf"] = nc_file
            logger.info("Exported radar data to NetCDF")

        else:
            logger.warning(f"Unknown format: {fmt}. Skipping.")

    return results
