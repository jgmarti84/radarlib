"""Tests comparing legacy and new PyART conversion implementations.

These tests verify that both bufr_to_pyart_legacy() and bufr_to_pyart()
produce equivalent Radar objects from the same BUFR data.
"""

from pathlib import Path

import numpy as np
import pytest


@pytest.mark.integration
@pytest.mark.filterwarnings("ignore:numpy.ndarray size changed.*:RuntimeWarning")
class TestLegacyVsNewPyartConversion:
    """Compare PyART Radar objects from legacy and new implementations."""

    @pytest.fixture
    def sample_bufr_files(self):
        """Get sample BUFR files from RMA5 directory."""
        data_dir = Path("tests/data/bufr/RMA5")
        if not data_dir.exists():
            pytest.skip("No BUFR RMA5 test data available")

        bufr_files = sorted(list(data_dir.glob("*.BUFR")))
        if len(bufr_files) < 2:
            pytest.skip("Need at least 2 BUFR files for testing")

        # Return first 2 files
        return [str(f) for f in bufr_files[:2]]

    def test_radar_objects_have_same_fields(self, sample_bufr_files):
        """Test that both implementations produce Radar with same field names."""
        from radarlib.io.bufr.bufr import bufr_to_dict
        from radarlib.io.bufr.legacy.pyart_legacy import bufr_to_pyart_legacy
        from radarlib.io.bufr.pyart_writer import bufr_to_pyart

        # try:
        #     import pyart
        # except ImportError:
        #     pytest.skip("pyart not installed")
        # Decode using legacy method (with legacy=True format)
        legacy_dicts = []
        for bufr_file in sample_bufr_files:
            decoded = bufr_to_dict(bufr_file, root_resources=None, logger_name="test", legacy=True)
            if decoded is not None:
                legacy_dicts.append(decoded)

        if not legacy_dicts:
            pytest.skip("Could not decode BUFR files with legacy format")

        # Create Radar using legacy implementation
        legacy_radar = bufr_to_pyart_legacy(fields=legacy_dicts, logger_name="test")

        # Decode using new method (with legacy=False format)
        new_dicts = []
        for bufr_file in sample_bufr_files:
            decoded = bufr_to_dict(bufr_file, root_resources=None, logger_name="test", legacy=False)
            if decoded is not None:
                new_dicts.append(decoded)

        if not new_dicts:
            pytest.skip("Could not decode BUFR files with new format")

        # Create Radar using new implementation
        new_radar = bufr_to_pyart(fields=new_dicts, logger_name="test")

        # Compare field names
        legacy_fields = set(legacy_radar.fields.keys())
        new_fields = set(new_radar.fields.keys())

        assert legacy_fields == new_fields, f"Field names mismatch. Legacy: {legacy_fields}, New: {new_fields}"

    def test_radar_dimensions_match(self, sample_bufr_files):
        """Test that both implementations produce Radar with same dimensions."""
        from radarlib.io.bufr.bufr import bufr_to_dict
        from radarlib.io.bufr.legacy.pyart_legacy import bufr_to_pyart_legacy
        from radarlib.io.bufr.pyart_writer import bufr_to_pyart

        # try:
        #     import pyart
        # except ImportError:
        #     pytest.skip("pyart not installed")
        # Decode and create radars
        legacy_dicts = [
            bufr_to_dict(f, root_resources=None, logger_name="test", legacy=True) for f in sample_bufr_files
        ]
        legacy_dicts = [d for d in legacy_dicts if d is not None]
        if not legacy_dicts:
            pytest.skip("Could not decode with legacy format")

        new_dicts = [bufr_to_dict(f, root_resources=None, logger_name="test", legacy=False) for f in sample_bufr_files]
        new_dicts = [d for d in new_dicts if d is not None]
        if not new_dicts:
            pytest.skip("Could not decode with new format")

        legacy_radar = bufr_to_pyart_legacy(fields=legacy_dicts, logger_name="test")
        new_radar = bufr_to_pyart(fields=new_dicts, logger_name="test")

        # Compare dimensions
        assert (
            legacy_radar.nrays == new_radar.nrays
        ), f"nrays mismatch: legacy={legacy_radar.nrays}, new={new_radar.nrays}"
        assert (
            legacy_radar.ngates == new_radar.ngates
        ), f"ngates mismatch: legacy={legacy_radar.ngates}, new={new_radar.ngates}"
        assert (
            legacy_radar.nsweeps == new_radar.nsweeps
        ), f"nsweeps mismatch: legacy={legacy_radar.nsweeps}, new={new_radar.nsweeps}"

    def test_radar_elevation_data_matches(self, sample_bufr_files):
        """Test that elevation data matches between implementations."""
        from radarlib.io.bufr.bufr import bufr_to_dict
        from radarlib.io.bufr.legacy.pyart_legacy import bufr_to_pyart_legacy
        from radarlib.io.bufr.pyart_writer import bufr_to_pyart

        # try:
        #     import pyart
        # except ImportError:
        #     pytest.skip("pyart not installed")

        legacy_dicts = [
            bufr_to_dict(f, root_resources=None, logger_name="test", legacy=True) for f in sample_bufr_files
        ]
        legacy_dicts = [d for d in legacy_dicts if d is not None]
        if not legacy_dicts:
            pytest.skip("Could not decode with legacy format")

        new_dicts = [bufr_to_dict(f, root_resources=None, logger_name="test", legacy=False) for f in sample_bufr_files]
        new_dicts = [d for d in new_dicts if d is not None]
        if not new_dicts:
            pytest.skip("Could not decode with new format")

        legacy_radar = bufr_to_pyart_legacy(fields=legacy_dicts, logger_name="test")
        new_radar = bufr_to_pyart(fields=new_dicts, logger_name="test")

        # Compare elevation data
        legacy_elev = legacy_radar.elevation["data"]
        new_elev = new_radar.elevation["data"]

        assert (
            legacy_elev.shape == new_elev.shape
        ), f"elevation shape mismatch: legacy={legacy_elev.shape}, new={new_elev.shape}"

        # Allow small numerical differences (1e-5)
        np.testing.assert_allclose(legacy_elev, new_elev, rtol=1e-5, atol=1e-5, err_msg="elevation data mismatch")

    def test_radar_azimuth_data_matches(self, sample_bufr_files):
        """Test that azimuth data matches between implementations."""
        from radarlib.io.bufr.bufr import bufr_to_dict
        from radarlib.io.bufr.legacy.pyart_legacy import bufr_to_pyart_legacy
        from radarlib.io.bufr.pyart_writer import bufr_to_pyart

        # try:
        #     import pyart
        # except ImportError:
        #     pytest.skip("pyart not installed")

        legacy_dicts = [
            bufr_to_dict(f, root_resources=None, logger_name="test", legacy=True) for f in sample_bufr_files
        ]
        legacy_dicts = [d for d in legacy_dicts if d is not None]
        if not legacy_dicts:
            pytest.skip("Could not decode with legacy format")

        new_dicts = [bufr_to_dict(f, root_resources=None, logger_name="test", legacy=False) for f in sample_bufr_files]
        new_dicts = [d for d in new_dicts if d is not None]
        if not new_dicts:
            pytest.skip("Could not decode with new format")

        legacy_radar = bufr_to_pyart_legacy(fields=legacy_dicts, logger_name="test")
        new_radar = bufr_to_pyart(fields=new_dicts, logger_name="test")

        # Compare azimuth data
        legacy_az = legacy_radar.azimuth["data"]
        new_az = new_radar.azimuth["data"]

        assert legacy_az.shape == new_az.shape, f"azimuth shape mismatch: legacy={legacy_az.shape}, new={new_az.shape}"

        np.testing.assert_allclose(legacy_az, new_az, rtol=1e-5, atol=1e-5, err_msg="azimuth data mismatch")

    def test_radar_range_data_matches(self, sample_bufr_files):
        """Test that range data matches between implementations."""
        from radarlib.io.bufr.bufr import bufr_to_dict
        from radarlib.io.bufr.legacy.pyart_legacy import bufr_to_pyart_legacy
        from radarlib.io.bufr.pyart_writer import bufr_to_pyart

        # try:
        #     import pyart
        # except ImportError:
        #     pytest.skip("pyart not installed")

        legacy_dicts = [
            bufr_to_dict(f, root_resources=None, logger_name="test", legacy=True) for f in sample_bufr_files
        ]
        legacy_dicts = [d for d in legacy_dicts if d is not None]
        if not legacy_dicts:
            pytest.skip("Could not decode with legacy format")

        new_dicts = [bufr_to_dict(f, root_resources=None, logger_name="test", legacy=False) for f in sample_bufr_files]
        new_dicts = [d for d in new_dicts if d is not None]
        if not new_dicts:
            pytest.skip("Could not decode with new format")

        legacy_radar = bufr_to_pyart_legacy(fields=legacy_dicts, logger_name="test")
        new_radar = bufr_to_pyart(fields=new_dicts, logger_name="test")

        # Compare range data
        legacy_range = legacy_radar.range["data"]
        new_range = new_radar.range["data"]

        assert (
            legacy_range.shape == new_range.shape
        ), f"range shape mismatch: legacy={legacy_range.shape}, new={new_range.shape}"

        np.testing.assert_allclose(legacy_range, new_range, rtol=1e-5, atol=1e-5, err_msg="range data mismatch")

    def test_radar_metadata_matches(self, sample_bufr_files):
        """Test that key metadata fields match between implementations."""
        from radarlib.io.bufr.bufr import bufr_to_dict
        from radarlib.io.bufr.legacy.pyart_legacy import bufr_to_pyart_legacy
        from radarlib.io.bufr.pyart_writer import bufr_to_pyart

        # try:
        #     import pyart
        # except ImportError:
        #     pytest.skip("pyart not installed")

        legacy_dicts = [
            bufr_to_dict(f, root_resources=None, logger_name="test", legacy=True) for f in sample_bufr_files
        ]
        legacy_dicts = [d for d in legacy_dicts if d is not None]
        if not legacy_dicts:
            pytest.skip("Could not decode with legacy format")

        new_dicts = [bufr_to_dict(f, root_resources=None, logger_name="test", legacy=False) for f in sample_bufr_files]
        new_dicts = [d for d in new_dicts if d is not None]
        if not new_dicts:
            pytest.skip("Could not decode with new format")

        legacy_radar = bufr_to_pyart_legacy(fields=legacy_dicts, logger_name="test")
        new_radar = bufr_to_pyart(fields=new_dicts, logger_name="test")

        # Compare key metadata fields
        key_metadata_fields = ["instrument_name", "platform_type", "institution"]

        for field in key_metadata_fields:
            legacy_val = legacy_radar.metadata.get(field)
            new_val = new_radar.metadata.get(field)

            assert legacy_val == new_val, f"metadata['{field}'] mismatch: legacy={legacy_val}, new={new_val}"

    def test_radar_field_data_matches(self, sample_bufr_files):
        """Test that field data values match between implementations.

        Note: The mask arrays may differ slightly due to implementation details.
        We check that the valid (non-masked) data values match.
        """
        from radarlib.io.bufr.bufr import bufr_to_dict
        from radarlib.io.bufr.legacy.pyart_legacy import bufr_to_pyart_legacy
        from radarlib.io.bufr.pyart_writer import bufr_to_pyart

        # try:
        #     import pyart
        # except ImportError:
        #     pytest.skip("pyart not installed")

        legacy_dicts = [
            bufr_to_dict(f, root_resources=None, logger_name="test", legacy=True) for f in sample_bufr_files
        ]
        legacy_dicts = [d for d in legacy_dicts if d is not None]
        if not legacy_dicts:
            pytest.skip("Could not decode with legacy format")

        new_dicts = [bufr_to_dict(f, root_resources=None, logger_name="test", legacy=False) for f in sample_bufr_files]
        new_dicts = [d for d in new_dicts if d is not None]
        if not new_dicts:
            pytest.skip("Could not decode with new format")

        legacy_radar = bufr_to_pyart_legacy(fields=legacy_dicts, logger_name="test")
        new_radar = bufr_to_pyart(fields=new_dicts, logger_name="test")

        # Compare field data for each field
        for field_name in legacy_radar.fields.keys():
            legacy_field = legacy_radar.fields[field_name]
            new_field = new_radar.fields[field_name]

            legacy_data = legacy_field["data"]
            new_data = new_field["data"]

            # Check shapes match
            assert legacy_data.shape == new_data.shape, (
                f"Field '{field_name}' data shape mismatch: " f"legacy={legacy_data.shape}, new={new_data.shape}"
            )

            # For masked arrays, get the valid data (not masked)
            if np.ma.isMaskedArray(legacy_data):
                legacy_valid = ~np.ma.getmaskarray(legacy_data)
            else:
                legacy_valid = np.ones_like(legacy_data, dtype=bool)

            if np.ma.isMaskedArray(new_data):
                new_valid = ~np.ma.getmaskarray(new_data)
            else:
                new_valid = np.ones_like(new_data, dtype=bool)

            # Check that at least some data is valid in both
            assert np.any(legacy_valid), f"Field '{field_name}' has all masked data in legacy"
            assert np.any(new_valid), f"Field '{field_name}' has all masked data in new"

            # Compare valid data values where both are valid
            both_valid = legacy_valid & new_valid
            if np.any(both_valid):
                np.testing.assert_allclose(
                    legacy_data[both_valid],
                    new_data[both_valid],
                    rtol=1e-5,
                    atol=1e-5,
                    err_msg=f"Field '{field_name}' data mismatch in valid regions",
                )

    def test_radar_coordinates_match(self, sample_bufr_files):
        """Test that latitude/longitude/altitude match between implementations."""
        from radarlib.io.bufr.bufr import bufr_to_dict
        from radarlib.io.bufr.legacy.pyart_legacy import bufr_to_pyart_legacy
        from radarlib.io.bufr.pyart_writer import bufr_to_pyart

        # try:
        #     import pyart
        # except ImportError:
        #     pytest.skip("pyart not installed")

        legacy_dicts = [
            bufr_to_dict(f, root_resources=None, logger_name="test", legacy=True) for f in sample_bufr_files
        ]
        legacy_dicts = [d for d in legacy_dicts if d is not None]
        if not legacy_dicts:
            pytest.skip("Could not decode with legacy format")

        new_dicts = [bufr_to_dict(f, root_resources=None, logger_name="test", legacy=False) for f in sample_bufr_files]
        new_dicts = [d for d in new_dicts if d is not None]
        if not new_dicts:
            pytest.skip("Could not decode with new format")

        legacy_radar = bufr_to_pyart_legacy(fields=legacy_dicts, logger_name="test")
        new_radar = bufr_to_pyart(fields=new_dicts, logger_name="test")

        # Compare coordinates
        legacy_lat = legacy_radar.latitude["data"][0]
        new_lat = new_radar.latitude["data"][0]
        assert abs(legacy_lat - new_lat) < 1e-5, f"latitude mismatch: legacy={legacy_lat}, new={new_lat}"

        legacy_lon = legacy_radar.longitude["data"][0]
        new_lon = new_radar.longitude["data"][0]
        assert abs(legacy_lon - new_lon) < 1e-5, f"longitude mismatch: legacy={legacy_lon}, new={new_lon}"

        legacy_alt = legacy_radar.altitude["data"][0]
        new_alt = new_radar.altitude["data"][0]
        assert abs(legacy_alt - new_alt) < 1e-5, f"altitude mismatch: legacy={legacy_alt}, new={new_alt}"
