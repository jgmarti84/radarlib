# -*- coding: utf-8 -*-
"""
COLMAX field generation for radar volumes.

Generates a COLMAX (column maximum) field by comparing gates across multiple
sweeps and retaining the maximum value for each gate in the reference sweep.
"""
import logging
from typing import Optional

import numpy as np
from pyart.config import get_field_name
from pyart.core import Radar

from radarlib import config
from radarlib.io.pyart.fieldfilters import filterfield_excluding_gates_above, filterfield_excluding_gates_below
from radarlib.io.pyart.vvg import get_ordered_sweep_list, get_vertical_vinculation_gate_map

logger = logging.getLogger(__name__)


def generate_colmax(
    radar: Radar,
    elev_limit: float = 0,
    source_field: Optional[str] = None,
    refl_filter: bool = False,
    refl_threshold: float = 10,
    rhv_filter: bool = True,
    rhv_threshold: float = 0.9,
    wrad_filter: bool = True,
    wrad_threshold: float = 4.2,
    zdr_filter: bool = True,
    zdr_threshold: float = 8.5,
    target_field: Optional[str] = None,
    save_changes: bool = False,
    path_out: Optional[str] = None,
    regenerate_flag: bool = False,
    root_cache: Optional[str] = None,
    verbose: bool = False,
) -> bool:
    """
    Generate COLMAX field by finding maximum values across sweeps for each gate.

    Parameters
    ----------
    radar : Radar
        PyART Radar object containing multiple sweeps.
    elev_limit : float, optional
        Elevation angle limit (degrees). Sweeps below this limit are excluded.
        Default is 0.
    source_field : str, optional
        Field name to use as source for COLMAX. If None, uses reflectivity.
    refl_filter : bool, optional
        Whether to filter by reflectivity threshold. Default is False.
    refl_threshold : float, optional
        Reflectivity threshold in dBZ. Default is 10.
    rhv_filter : bool, optional
        Whether to filter by RhoHV threshold. Default is True.
    rhv_threshold : float, optional
        RhoHV threshold (0-1). Default is 0.9.
    wrad_filter : bool, optional
        Whether to filter by spectrum width threshold. Default is True.
    wrad_threshold : float, optional
        Spectrum width threshold. Default is 4.2.
    zdr_filter : bool, optional
        Whether to filter by ZDR threshold. Default is True.
    zdr_threshold : float, optional
        ZDR threshold. Default is 8.5.
    target_field : str, optional
        Name for the output COLMAX field. If None, uses 'colmax'.
    save_changes : bool, optional
        Whether to save the modified radar to file. Default is False.
    path_out : str, optional
        Output path for saving. Required if save_changes is True.
    regenerate_flag : bool, optional
        Whether to regenerate cached vertical vinculation map. Default is False.
    root_cache : str, optional
        Root cache directory path. If None, uses config.ROOT_CACHE_PATH.
    verbose : bool, optional
        Enable verbose logging. Default is False.

    Returns
    -------
    bool
        True if COLMAX field was successfully generated, False otherwise.

    Raises
    ------
    ValueError
        If radar has fewer than 2 sweeps or source field is missing.
    """
    # Initialize field names
    if source_field is None:
        source_field = get_field_name("reflectivity")

    if target_field is None:
        target_field = get_field_name("colmax")

    refl_field = get_field_name("reflectivity")
    rhv_field = get_field_name("cross_correlation_ratio")
    zdr_field = get_field_name("differential_reflectivity")
    wrad_field = get_field_name("spectrum_width")

    if root_cache is None:
        root_cache = config.ROOT_CACHE_PATH

    # Validate input
    if source_field not in radar.fields:
        logger.error(f"Source field '{source_field}' not found in radar fields.")
        return False

    if radar.nsweeps < 2:
        logger.debug("Cannot generate COLMAX: volume has fewer than 2 sweeps.")
        return False

    # Create filtered copy of source field
    filtered_field_name = source_field + "_filtered"
    _apply_polarimetric_filters(
        radar=radar,
        source_field=source_field,
        target_field=filtered_field_name,
        refl_field=refl_field,
        refl_filter=refl_filter,
        refl_threshold=refl_threshold,
        rhv_field=rhv_field,
        rhv_filter=rhv_filter,
        rhv_threshold=rhv_threshold,
        zdr_field=zdr_field,
        zdr_filter=zdr_filter,
        zdr_threshold=zdr_threshold,
        wrad_field=wrad_field,
        wrad_filter=wrad_filter,
        wrad_threshold=wrad_threshold,
    )

    # Get sweep ordering and vertical vinculation map
    sw_tuples_az, sweep_ref = get_ordered_sweep_list(radar, elev_limit)
    vvg_map = get_vertical_vinculation_gate_map(
        radar=radar,
        logger_name=logger.name,
        use_sweeps_above=elev_limit,
        save_vvg_map=True,
        root_cache=root_cache,
        verbose=verbose,
        regenerate_flag=regenerate_flag,
    )

    # Generate COLMAX field
    colmax_data = _compute_colmax(
        radar=radar,
        filtered_field_name=filtered_field_name,
        source_field=source_field,
        sw_tuples_az=sw_tuples_az,
        sweep_ref=sweep_ref,
        vvg_map=vvg_map,
    )

    # Add field to radar
    _add_colmax_to_radar(
        radar=radar,
        colmax_data=colmax_data,
        source_field=filtered_field_name,
        target_field=target_field,
    )

    # Clean up temporary field
    if filtered_field_name in radar.fields:
        del radar.fields[filtered_field_name]

    if save_changes and path_out:
        from radarlib.io.pyart.pyart_radar import save_radar_netcdf

        save_radar_netcdf(radar=radar, path_out=path_out)

    return True


def _apply_polarimetric_filters(
    radar: Radar,
    source_field: str,
    target_field: str,
    refl_field: str,
    refl_filter: bool,
    refl_threshold: float,
    rhv_field: str,
    rhv_filter: bool,
    rhv_threshold: float,
    zdr_field: str,
    zdr_filter: bool,
    zdr_threshold: float,
    wrad_field: str,
    wrad_filter: bool,
    wrad_threshold: float,
) -> None:
    """Apply polarimetric filters to the source field."""
    src_field_data = radar.fields[source_field]["data"]

    radar.add_field_like(
        source_field,
        target_field,
        src_field_data.copy(),
        replace_existing=True,
    )
    radar.fields[target_field]["standard_name"] = radar.fields[source_field].get("standard_name", source_field)
    radar.fields[target_field]["units"] = radar.fields[source_field].get("units", "")

    # Apply reflectivity filter
    if refl_filter and refl_field in radar.fields:
        try:
            filterfield_excluding_gates_below(
                radar=radar,
                threshold=refl_threshold,
                source_field=refl_field,
                target_fields=[target_field],
                overwrite_fields=True,
                logger_name=logger.name,
            )
        except Exception as e:
            logger.warning(f"Error filtering with {refl_field}: {e}")

    # Apply RhoHV filter
    if rhv_filter and rhv_field in radar.fields:
        try:
            filterfield_excluding_gates_below(
                radar=radar,
                threshold=rhv_threshold,
                source_field=rhv_field,
                target_fields=[target_field],
                overwrite_fields=True,
                logger_name=logger.name,
            )
        except Exception as e:
            logger.warning(f"Error filtering with {rhv_field}: {e}")

    # Apply ZDR filter
    if zdr_filter and zdr_field in radar.fields:
        try:
            filterfield_excluding_gates_above(
                radar=radar,
                threshold=zdr_threshold,
                source_field=zdr_field,
                target_fields=[target_field],
                overwrite_fields=True,
                logger_name=logger.name,
            )
        except Exception as e:
            logger.warning(f"Error filtering with {zdr_field}: {e}")

    # Apply spectrum width filter
    if wrad_filter and wrad_field in radar.fields:
        try:
            filterfield_excluding_gates_above(
                radar=radar,
                threshold=wrad_threshold,
                source_field=wrad_field,
                target_fields=[target_field],
                overwrite_fields=True,
                logger_name=logger.name,
            )
        except Exception as e:
            logger.warning(f"Error filtering with {wrad_field}: {e}")


def _compute_colmax(
    radar: Radar,
    filtered_field_name: str,
    source_field: str,
    sw_tuples_az: list,
    sweep_ref: int,
    vvg_map: np.ndarray,
) -> np.ma.MaskedArray:
    """
    Compute COLMAX by comparing gates across sweeps.

    Returns
    -------
    np.ma.MaskedArray
        COLMAX data with shape (nrays_in_sweep, ngates).
    """
    sw_rays = int(radar.nrays / radar.nsweeps)
    filtered_data = radar.fields[filtered_field_name]["data"]

    # Initialize with reference sweep
    radar_aux = radar.extract_sweeps([sweep_ref])
    colmax_data = radar_aux.fields[filtered_field_name]["data"].copy()
    del radar_aux

    # Compare gates across sweeps
    for gate_ref in range(radar.ngates):
        for _el, sweep in sw_tuples_az[1:]:
            gate = vvg_map[gate_ref, sweep]

            if np.ma.is_masked(gate):
                continue

            gate = int(gate)

            for ray in range(sw_rays):
                ray_idx = ray + sw_rays * sweep

                # If reference gate is masked but higher sweep has valid data
                if np.ma.is_masked(colmax_data[ray, gate_ref]) and not np.ma.is_masked(filtered_data[ray_idx, gate]):
                    colmax_data[ray, gate_ref] = filtered_data[ray_idx, gate]
                    colmax_data.mask[ray, gate_ref] = False

                # If higher sweep value is greater, update
                elif (
                    not np.ma.is_masked(filtered_data[ray_idx, gate])
                    and not np.ma.is_masked(colmax_data[ray, gate_ref])
                    and colmax_data[ray, gate_ref] < filtered_data[ray_idx, gate]
                ):
                    colmax_data[ray, gate_ref] = filtered_data[ray_idx, gate]

    return colmax_data


def _add_colmax_to_radar(
    radar: Radar,
    colmax_data: np.ma.MaskedArray,
    source_field: str,
    target_field: str,
) -> None:
    """Add COLMAX field to radar object with proper metadata."""
    # Resize to full volume dimensions
    colmax_field_data = np.ma.array(np.zeros((radar.nrays, radar.ngates)), mask=True)
    sw_rays = int(radar.nrays / radar.nsweeps)
    colmax_field_data.mask[0:sw_rays, :] = colmax_data.mask
    colmax_field_data[0:sw_rays, :] = colmax_data.copy()

    # Add field to radar
    radar.add_field_like(source_field, target_field, colmax_field_data, replace_existing=True)

    # Set metadata
    radar.fields[target_field]["standard_name"] = target_field
    radar.fields[target_field]["long_name"] = "Column Maximum"
    radar.fields[target_field]["units"] = radar.fields[source_field].get("units", "")
