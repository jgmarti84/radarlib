"""
Utilities for comparing outputs from legacy and new BUFR to PyART implementations.
"""

from typing import Any, Dict

import numpy as np


def compare_decoded_dicts(
    legacy_dict: Dict[str, Any],
    new_dict: Dict[str, Any],
    tolerance: float = 1e-6,
) -> Dict[str, Any]:
    """
    Compare two decoded BUFR dictionaries from legacy and new implementations.

    Parameters
    ----------
    legacy_dict : Dict[str, Any]
        Decoded BUFR dict from legacy implementation (legacy format)
    new_dict : Dict[str, Any]
        Decoded BUFR dict from new implementation (new format)
    tolerance : float, optional
        Tolerance for floating-point comparisons (default: 1e-6)

    Returns
    -------
    Dict[str, Any]
        Dictionary with comparison results and any discrepancies
    """
    report = {
        "overall_match": True,
        "discrepancies": [],
        "metadata_comparison": {},
        "field_comparison": {},
        "field_names_match": True,
        "field_shapes_match": True,
        "field_values_match": True,
    }

    # Extract fields from both formats
    if isinstance(legacy_dict, dict) and "metadata_vol" in legacy_dict:
        # Legacy format
        legacy_metadata = legacy_dict.get("metadata_vol", {})
        legacy_fields = None  # Legacy doesn't have direct field dict
    else:
        legacy_metadata = legacy_dict.get("metadata", {})
        legacy_fields = legacy_dict.get("fields", {})

    new_metadata = new_dict.get("metadata", {})
    new_fields = new_dict.get("fields", {})

    # Compare metadata
    metadata_keys_to_check = [
        "year",
        "month",
        "day",
        "hour",
        "minute",
        "latitude",
        "longitude",
        "altitude",
        "nsweeps",
        "radar_name",
    ]

    for key in metadata_keys_to_check:
        legacy_val = legacy_metadata.get(key)
        new_val = new_metadata.get(key)

        if legacy_val is None or new_val is None:
            # One or both don't have this key
            continue

        # For numeric comparisons
        try:
            if isinstance(legacy_val, (int, float)) and isinstance(new_val, (int, float)):
                if abs(legacy_val - new_val) > tolerance:
                    report["overall_match"] = False
                    report["discrepancies"].append(f"Metadata '{key}' mismatch: legacy={legacy_val}, new={new_val}")
                    report["metadata_comparison"][key] = {
                        "matches": False,
                        "legacy": legacy_val,
                        "new": new_val,
                    }
                else:
                    report["metadata_comparison"][key] = {"matches": True}
            elif legacy_val == new_val:
                report["metadata_comparison"][key] = {"matches": True}
            else:
                report["overall_match"] = False
                report["discrepancies"].append(f"Metadata '{key}' mismatch: legacy={legacy_val}, new={new_val}")
                report["metadata_comparison"][key] = {
                    "matches": False,
                    "legacy": legacy_val,
                    "new": new_val,
                }
        except Exception as e:
            report["discrepancies"].append(f"Error comparing metadata '{key}': {e}")

    # Compare fields if both have them
    if legacy_fields is not None and new_fields is not None:
        legacy_field_names = set(legacy_fields.keys())
        new_field_names = set(new_fields.keys())

        if legacy_field_names != new_field_names:
            report["overall_match"] = False
            report["field_names_match"] = False
            report["discrepancies"].append(f"Field names mismatch: legacy={legacy_field_names}, new={new_field_names}")

        # Compare field values for common fields
        for field_name in legacy_field_names & new_field_names:
            legacy_data = (
                legacy_fields[field_name].get("data")
                if isinstance(legacy_fields[field_name], dict)
                else legacy_fields[field_name]
            )
            new_data = (
                new_fields[field_name].get("data")
                if isinstance(new_fields[field_name], dict)
                else new_fields[field_name]
            )

            if legacy_data is None or new_data is None:
                continue

            if legacy_data.shape != new_data.shape:
                report["overall_match"] = False
                report["field_shapes_match"] = False
                report["discrepancies"].append(
                    f"Field '{field_name}' shape mismatch: " f"legacy={legacy_data.shape}, new={new_data.shape}"
                )
            else:
                # Compare values with tolerance
                try:
                    max_diff = np.nanmax(np.abs(legacy_data - new_data))
                    matches = np.allclose(legacy_data, new_data, atol=tolerance, equal_nan=True)

                    report["field_comparison"][field_name] = {
                        "shape": legacy_data.shape,
                        "matches": bool(matches),
                        "max_difference": float(max_diff),
                    }

                    if not matches:
                        report["overall_match"] = False
                        report["field_values_match"] = False
                        report["discrepancies"].append(f"Field '{field_name}' values mismatch (max diff: {max_diff})")
                except Exception as e:
                    report["discrepancies"].append(f"Error comparing field '{field_name}': {e}")

    return report


def compare_radar_objects(legacy_radar, new_radar, tolerance: float = 1e-6) -> Dict[str, Any]:
    """
    Compare two PyART Radar objects and return a detailed comparison report.

    Parameters
    ----------
    legacy_radar : pyart.Radar
        Radar object from legacy implementation
    new_radar : pyart.Radar
        Radar object from new implementation
    tolerance : float, optional
        Tolerance for floating-point comparisons (default: 1e-6)

    Returns
    -------
    Dict[str, Any]
        Dictionary with comparison results and any discrepancies
    """
    report = {
        "overall_match": True,
        "discrepancies": [],
        "field_comparison": {},
        "coordinate_comparison": {},
        "metadata_comparison": {},
    }

    # Compare field names
    legacy_fields = set(legacy_radar.fields.keys()) if legacy_radar.fields else set()
    new_fields = set(new_radar.fields.keys()) if new_radar.fields else set()

    if legacy_fields != new_fields:
        report["overall_match"] = False
        report["discrepancies"].append(f"Field names mismatch: legacy={legacy_fields}, new={new_fields}")
    else:
        # Compare field values
        for field_name in legacy_fields:
            legacy_data = legacy_radar.fields[field_name]["data"]
            new_data = new_radar.fields[field_name]["data"]

            if legacy_data.shape != new_data.shape:
                report["overall_match"] = False
                report["discrepancies"].append(
                    f"Field '{field_name}' shape mismatch: " f"legacy={legacy_data.shape}, new={new_data.shape}"
                )
            else:
                # Compare values with tolerance
                max_diff = np.nanmax(np.abs(legacy_data - new_data))
                matches = np.allclose(legacy_data, new_data, atol=tolerance, equal_nan=True)
                report["field_comparison"][field_name] = {
                    "shape": legacy_data.shape,
                    "matches": bool(matches),
                    "max_difference": float(max_diff),
                }
                if not matches:
                    report["overall_match"] = False
                    report["discrepancies"].append(f"Field '{field_name}' values mismatch (max diff: {max_diff})")

    # Compare coordinates
    coords_to_check = ["latitude", "longitude", "altitude", "time"]
    for coord in coords_to_check:
        if hasattr(legacy_radar, coord) and hasattr(new_radar, coord):
            legacy_coord = getattr(legacy_radar, coord)
            new_coord = getattr(new_radar, coord)

            if isinstance(legacy_coord["data"], np.ndarray):
                if not np.allclose(legacy_coord["data"], new_coord["data"], atol=tolerance, equal_nan=True):
                    report["overall_match"] = False
                    report["discrepancies"].append(f"Coordinate '{coord}' mismatch")
                    report["coordinate_comparison"][coord] = {"matches": False}
                else:
                    report["coordinate_comparison"][coord] = {"matches": True}

    return report


def compare_netcdf_files(legacy_file: str, new_file: str) -> Dict[str, Any]:
    """
    Compare two NetCDF files written from both implementations.

    Parameters
    ----------
    legacy_file : str
        Path to NetCDF file from legacy implementation
    new_file : str
        Path to NetCDF file from new implementation

    Returns
    -------
    Dict[str, Any]
        Comparison results
    """
    try:
        import netCDF4
    except ImportError:
        raise ImportError("netCDF4 is required for file comparison")

    report = {
        "overall_match": True,
        "discrepancies": [],
    }

    with netCDF4.Dataset(legacy_file, "r") as legacy_ds, netCDF4.Dataset(new_file, "r") as new_ds:
        # Compare dimensions
        if legacy_ds.dimensions.keys() != new_ds.dimensions.keys():
            report["overall_match"] = False
            report["discrepancies"].append("Dimension names mismatch")

        # Compare variables
        if legacy_ds.variables.keys() != new_ds.variables.keys():
            report["overall_match"] = False
            report["discrepancies"].append("Variable names mismatch")

    return report


def nested_equal(obj1, obj2, path="root"):
    """Recursively compare dicts/lists/numpy arrays."""
    if isinstance(obj1, np.ndarray) or isinstance(obj2, np.ndarray):
        if not np.array_equal(obj1, obj2):
            raise AssertionError(f"Mismatch at {path}")
    elif isinstance(obj1, dict) and isinstance(obj2, dict):
        if set(obj1.keys()) != set(obj2.keys()):
            raise AssertionError(f"Key mismatch at {path}")
        for k in obj1:
            nested_equal(obj1[k], obj2[k], path=f"{path}.{k}")
    elif isinstance(obj1, (list, tuple)) and isinstance(obj2, (list, tuple)):
        if len(obj1) != len(obj2):
            raise AssertionError(f"List length mismatch at {path}")
        for i, (a, b) in enumerate(zip(obj1, obj2)):
            nested_equal(a, b, path=f"{path}[{i}]")
    else:
        if obj1 != obj2:
            raise AssertionError(f"Value mismatch at {path}: {obj1} != {obj2}")
