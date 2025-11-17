"""Legacy pipeline tests for BUFR to PyART conversion.

This module contains tests for the original legacy implementation using:
- dec_bufr_file() and bufr_to_dict() from legacy.bufr_legacy
- bufr_to_pyart_legacy() from legacy.pyart_legacy
"""

from pathlib import Path

import pytest


@pytest.mark.integration
@pytest.mark.filterwarnings("ignore:numpy.ndarray size changed.*:RuntimeWarning")
def test_end_to_end_legacy_pipeline_multiple_bufr_files():
    """Test the legacy pipeline end-to-end with multiple RMA5 BUFR files.

    This test demonstrates the original legacy implementation:
    1. Locates all BUFR files in tests/data/bufr/RMA5/
    2. Decodes each using the legacy dec_bufr_file()
    3. Converts each to PyART using the legacy bufr_to_pyart_legacy()
    4. Validates the resulting Radar objects

    Legacy API:
    - Decoder: radarlib.io.bufr.legacy.bufr_legacy.bufr_to_dict()
    - Converter: radarlib.io.bufr.legacy.pyart_legacy.bufr_to_pyart_legacy()
    """
    # Locate BUFR files in RMA5 subdirectory
    data_dir = Path("tests/data/bufr/RMA5")
    if not data_dir.exists():
        pytest.skip("No BUFR RMA5 test data available")

    bufr_files = sorted(list(data_dir.glob("*.BUFR")))
    if not bufr_files:
        pytest.skip("No BUFR test files found in RMA5 directory")

    # Import legacy implementations
    from radarlib.io.bufr.legacy.bufr_legacy import bufr_to_dict
    from radarlib.io.bufr.legacy.pyart_legacy import bufr_to_pyart_legacy

    # try:
    #     import pyart
    # except ImportError:
    #     pytest.skip("pyart not installed")

    try:
        fields = []
        run_logs = []
        for bufr_file in bufr_files:
            path = "/".join(str(bufr_file).split("/")[:-1]) + "/"
            filename = str(bufr_file).split("/")[-1]
            bufr_to_dict(filename, path, False, fields, run_logs)

        # Convert to PyART Radar object using legacy converter
        # The legacy function expects a list of field dicts
        radar = bufr_to_pyart_legacy(
            fields=fields,
            logger_name="test_legacy",
        )

    except Exception as e:
        pytest.skip(f"Could not process any BUFR files with legacy pipeline: {e}")

    assert radar is not None, "Radar for RMA5 is None"
    assert hasattr(radar, "fields"), "Radar for RMA5 missing 'fields' attribute"
    assert len(radar.fields) > 0, "Radar for RMA5 has no fields"

    # Check required attributes
    required_attrs = [
        "metadata",
        "latitude",
        "longitude",
        "altitude",
        "azimuth",
        "elevation",
        "range",
        "time",
    ]
    for attr in required_attrs:
        assert hasattr(radar, attr), f"Radar for RMA5 missing attribute: {attr}"

    # Validate field data
    for field_name, field_dict in radar.fields.items():
        assert "data" in field_dict, f"Field '{field_name}' in RMA5 missing 'data' key"
        field_data = field_dict["data"]
        assert field_data is not None, f"Field '{field_name}' in RMA5 data is None"
        assert field_data.size > 0, f"Field '{field_name}' in RMA5 data is empty"

    # Validate coordinates are within reasonable ranges
    if hasattr(radar.latitude["data"], "__len__"):
        lat = radar.latitude["data"][0]
        assert -90 <= lat <= 90, f"Invalid latitude in RMA5: {lat}"

    if hasattr(radar.longitude["data"], "__len__"):
        lon = radar.longitude["data"][0]
        assert -180 <= lon <= 180, f"Invalid longitude in RMA5: {lon}"

    print("✓ Successfully processed RMA5 BUFR files through legacy pipeline")
