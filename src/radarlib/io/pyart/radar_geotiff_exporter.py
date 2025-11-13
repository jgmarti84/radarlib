"""
GeoTIFF export functionality for radar data using rasterio.

This module provides functions to save radar PPI sweeps as georeferenced GeoTIFF files
with proper coordinate system and geotransform information.
"""

import logging
import os
from typing import Tuple

import numpy as np
from pyart.core import Radar

logger = logging.getLogger(__name__)


def _get_ppi_grid(radar: Radar, sweep: int = 0) -> Tuple[np.ndarray, np.ndarray]:
    """
    Get X and Y coordinate grids for a PPI sweep.

    Parameters
    ----------
    radar : Radar
        PyART radar object
    sweep : int
        Sweep index (default: 0)

    Returns
    -------
    x_grid : ndarray
        X coordinates (meters from radar center)
    y_grid : ndarray
        Y coordinates (meters from radar center)
    """
    # Get sweep data
    sweep_start = radar.sweep_start_ray_index["data"][sweep]
    sweep_end = radar.sweep_end_ray_index["data"][sweep]

    # Get azimuth and range for this sweep
    azimuths = radar.azimuth["data"][sweep_start : sweep_end + 1]
    ranges = radar.range["data"]

    # Create meshgrid
    azimuth_grid, range_grid = np.meshgrid(np.deg2rad(azimuths), ranges, indexing="ij")

    # Convert to Cartesian coordinates
    x_grid = range_grid * np.sin(azimuth_grid)
    y_grid = range_grid * np.cos(azimuth_grid)

    return x_grid, y_grid


def _get_geotransform(
    x_grid: np.ndarray, y_grid: np.ndarray, radar: Radar
) -> Tuple[float, float, float, float, float, float]:
    """
    Calculate geotransform parameters for georeferencing.

    Parameters
    ----------
    x_grid : ndarray
        X coordinate grid
    y_grid : ndarray
        Y coordinate grid
    radar : Radar
        PyART radar object

    Returns
    -------
    geotransform : tuple
        (x_origin, pixel_width, 0, y_origin, 0, pixel_height)
    """
    # Get radar location
    lat0 = radar.latitude["data"][0]
    lon0 = radar.longitude["data"][0]
    # alt0 = radar.altitude["data"][0] if hasattr(radar.altitude["data"], "__len__") else 0

    # Get grid spacing (approximate)
    dx = np.abs(x_grid[0, 1] - x_grid[0, 0]) if x_grid.shape[1] > 1 else 500
    dy = np.abs(y_grid[1, 0] - y_grid[0, 0]) if y_grid.shape[0] > 1 else 500

    # Get grid origin (top-left corner)
    # x_min = x_grid.min()
    # y_max = y_grid.max()

    return (lon0, dx / 111000, 0, lat0, 0, -dy / 111000)  # Approximate lat/lon conversion


def save_ppi_field_to_geotiff(
    radar: Radar,
    field: str,
    output_path: str,
    filename: str,
    sweep: int = 0,
    crs: str = "EPSG:4326",
) -> str:
    """
    Save a PPI field as a georeferenced GeoTIFF file.

    Parameters
    ----------
    radar : Radar
        PyART radar object
    field : str
        Name of the radar field to export
    output_path : str
        Directory where to save the GeoTIFF
    filename : str
        Name of the output file (should end with .tif or .tiff)
    sweep : int
        Sweep index to export (default: 0)
    crs : str
        Coordinate reference system EPSG code (default: "EPSG:4326" for WGS84)

    Returns
    -------
    str
        Full path to saved GeoTIFF file

    Raises
    ------
    ValueError
        If field not in radar or sweep index invalid
    ImportError
        If rasterio is not installed
    """
    try:
        import rasterio
        from rasterio.transform import Affine
    except ImportError:
        raise ImportError("rasterio is required for GeoTIFF export. " "Install it with: pip install rasterio")

    if field not in radar.fields:
        raise ValueError(f"Field '{field}' not found in radar. Available fields: {list(radar.fields.keys())}")

    if sweep >= radar.nsweeps:
        raise ValueError(f"Sweep {sweep} out of range. Radar has {radar.nsweeps} sweeps.")

    # Create directory if needed
    os.makedirs(output_path, exist_ok=True)

    # Get field data for this sweep
    sweep_start = radar.sweep_start_ray_index["data"][sweep]
    sweep_end = radar.sweep_end_ray_index["data"][sweep]
    field_data = radar.fields[field]["data"][sweep_start : sweep_end + 1, :]

    # Get coordinate grids
    x_grid, y_grid = _get_ppi_grid(radar, sweep)

    # Get geotransform
    geotransform = _get_geotransform(x_grid, y_grid, radar)

    # Create transform for rasterio
    transform = Affine(
        geotransform[1],  # pixel width
        geotransform[2],  # rotation (0)
        geotransform[0],  # x origin
        geotransform[4],  # rotation (0)
        geotransform[5],  # pixel height (negative)
        geotransform[3],  # y origin
    )

    # Convert to uint16 or float32 based on data type
    if np.issubdtype(field_data.dtype, np.floating):
        output_dtype = rasterio.float32
    else:
        output_dtype = rasterio.uint16

    # Prepare output data (handle masked arrays)
    if hasattr(field_data, "mask"):
        data = field_data.filled(np.nan)
    else:
        data = field_data.copy()

    # Full output path
    full_path = os.path.join(output_path, filename)

    # Write GeoTIFF
    with rasterio.open(
        full_path,
        "w",
        driver="GTiff",
        height=data.shape[0],
        width=data.shape[1],
        count=1,  # Single band
        dtype=output_dtype,
        crs=crs,
        transform=transform,
        nodata=np.nan if output_dtype == rasterio.float32 else 0,
    ) as dst:
        dst.write(data, 1)

    logger.info(f"Saved GeoTIFF: {full_path}")
    return full_path


def save_multiple_fields_to_geotiff(
    radar: Radar,
    fields: list,
    output_base_path: str,
    sweep: int = 0,
    crs: str = "EPSG:4326",
) -> dict:
    """
    Save multiple PPI fields as georeferenced GeoTIFF files.

    Parameters
    ----------
    radar : Radar
        PyART radar object
    fields : list
        List of field names to export
    output_base_path : str
        Base directory for output files
    sweep : int
        Sweep index to export (default: 0)
    crs : str
        Coordinate reference system EPSG code (default: "EPSG:4326" for WGS84)

    Returns
    -------
    dict
        Dictionary mapping field name to output file path

    Examples
    --------
    >>> results = save_multiple_fields_to_geotiff(
    ...     radar,
    ...     ['DBZH', 'VRAD', 'KDP'],
    ...     'output_geotiff/',
    ...     sweep=0
    ... )
    >>> for field, filepath in results.items():
    ...     print(f"{field}: {filepath}")
    """
    results = {}

    for field in fields:
        if field not in radar.fields:
            logger.warning(f"Field '{field}' not in radar. Skipping.")
            continue

        try:
            filename = f"{field}_sweep{sweep:02d}.tif"
            output_path = save_ppi_field_to_geotiff(radar, field, output_base_path, filename, sweep=sweep, crs=crs)
            results[field] = output_path
            logger.info(f"Successfully exported field: {field}")

        except Exception as e:
            logger.error(f"Error exporting field '{field}': {e}")
            continue

    return results


def radar_to_netcdf_with_coordinates(radar: Radar, output_path: str, filename: str = "radar_data.nc") -> str:
    """
    Save radar data as NetCDF with full coordinate information.

    This is useful for preserving all radar data and coordinates for later
    processing or analysis.

    Parameters
    ----------
    radar : Radar
        PyART radar object
    output_path : str
        Directory where to save the NetCDF
    filename : str
        Name of the output file (default: "radar_data.nc")

    Returns
    -------
    str
        Full path to saved NetCDF file
    """
    import os

    os.makedirs(output_path, exist_ok=True)
    full_path = os.path.join(output_path, filename)

    try:
        import pyart

        pyart.io.cfradial.write_cfradial(full_path, radar)
        logger.info(f"Saved NetCDF: {full_path}")
        return full_path

    except Exception as e:
        logger.error(f"Error saving NetCDF: {e}")
        raise
