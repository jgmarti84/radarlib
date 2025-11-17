"""Consistency tests comparing legacy and new modules.

This module contains integration tests that compare:
- Legacy decoder: radarlib.io.bufr.legacy.bufr_legacy.bufr_to_dict()
- New decoder: radarlib.io.bufr.bufr.bufr_to_dict()
- Legacy radar generator: radarlib.io.bufr.legacy.pyart_legacy.bufr_to_pyart_legacy()
- New radar generator: radarlib.io.bufr.pyart_writer.bufr_to_pyart()

Tests validate that both produce equivalent output and that
the new decoder is a proper replacement for the legacy one.
"""

from pathlib import Path

import numpy as np
import pytest

# Import both implementations
from radarlib.io.bufr.bufr import bufr_to_dict, dec_bufr_file
from radarlib.io.bufr.legacy import bufr_to_dict as legacy_bufr_to_dict
from radarlib.io.bufr.legacy import dec_bufr_file as legacy_dec_bufr_file


@pytest.mark.integration
class TestBUFRDecoding:
    """Test BUFR decoding between implementations."""

    @pytest.fixture
    def sample_bufr_file(self, bufr_test_dir):
        file = bufr_test_dir / "AR5_1000_1_DBZH_20240101T000746Z.BUFR"
        if not file.exists():
            pytest.skip("Sample BUFR file not available in tests/data/bufr")
        return str(file)

    def test_dec_bufr_equivalence(self, sample_bufr_file):
        """Test that new and legacy BUFR decoders produce equivalent dicts."""

        vol_metadata, sweeps_data, vol_data, run_log = dec_bufr_file(sample_bufr_file, logger_name="test_new_decoder")
        vol_metadata_legacy, sweeps_data_legacy, vol_data_legacy, run_log_legacy = legacy_dec_bufr_file(
            sample_bufr_file, logger_name="test_legacy_decoder"
        )

        from tests.integration.legacy.legacy_comparison_utils import nested_equal

        metadata_set = {"year", "month", "day", "hour", "min", "lat", "lon", "radar_height", "nsweeps"}
        vol_metadata = {k: vol_metadata[k] for k in metadata_set}
        nested_equal(vol_metadata, vol_metadata_legacy)

        sweeps_set = {
            "year_ini",
            "month_ini",
            "day_ini",
            "hour_ini",
            "min_ini",
            "sec_ini",
            "year",
            "month",
            "day",
            "hour",
            "min",
            "sec",
            "elevation",
            "ngates",
            "range_size",
            "range_offset",
            "nrays",
            "antenna_beam_az",
            "compress_data",
            "data",
        }
        sweeps_data = [{k: sweep[k] for k in sweeps_set} for sweep in sweeps_data]
        sweeps_data_legacy = [{k: sweep[k] for k in sweeps_set} for sweep in sweeps_data_legacy]
        nested_equal(sweeps_data[0], sweeps_data_legacy[0])

    def test_bufr_to_dict_equivalence(self, sample_bufr_file):
        """Test that new and legacy BUFR dict generation funcs produce equivalent dicts."""
        # Decode using new implementation
        # We need to pass legacy=True to get the same format as legacy implementation and have comparable outputs
        bufr_dict = bufr_to_dict(sample_bufr_file, logger_name="test_new_decoder", legacy=True)

        # Decode using legacy implementation
        path = "/".join(sample_bufr_file.split("/")[:-1]) + "/"
        filename = sample_bufr_file.split("/")[-1]
        vols = []
        run_logs = []
        legacy_bufr_to_dict(
            filename=filename,
            path=path,
            debug=False,
            volumenes=vols,
            run_logs=run_logs,
            logger_name="test_legacy_decoder",
        )
        bufr_dict_legacy = vols[0]

        # Basic checks
        assert bufr_dict is not None
        assert bufr_dict_legacy is not None
        assert "data" in bufr_dict and "info" in bufr_dict
        assert "data" in bufr_dict_legacy and "info" in bufr_dict_legacy

        from tests.integration.legacy.legacy_comparison_utils import nested_equal

        nested_equal(bufr_dict["data"], bufr_dict_legacy["data"], "data")
        nested_equal(bufr_dict["info"], bufr_dict_legacy["info"], "info")

    def test_bufr_data_equivalence(self, sample_bufr_file):
        """Test that legacy format data matches legacy decoder output."""
        # Decode using new implementation
        # We need to pass legacy=True to get the same format as legacy implementation and have comparable outputs
        bufr_dict = bufr_to_dict(sample_bufr_file, logger_name="test_new_decoder", legacy=True)

        # Decode using legacy implementation
        path = "/".join(sample_bufr_file.split("/")[:-1]) + "/"
        filename = sample_bufr_file.split("/")[-1]
        vols = []
        run_logs = []
        legacy_bufr_to_dict(
            filename=filename,
            path=path,
            debug=False,
            volumenes=vols,
            run_logs=run_logs,
            logger_name="test_legacy_decoder",
        )
        bufr_dict_legacy = vols[0]

        new_data = bufr_dict["data"]  # type: ignore
        legacy_data = bufr_dict_legacy["data"]

        # Should have same shape
        assert (
            new_data.shape == legacy_data.shape
        ), f"Data shape mismatch: new={new_data.shape}, legacy={legacy_data.shape}"

        # Handle masked arrays
        if isinstance(new_data, np.ma.MaskedArray):
            new_valid = new_data.compressed()
        else:
            new_valid = new_data[np.isfinite(new_data)]

        if isinstance(legacy_data, np.ma.MaskedArray):
            legacy_valid = legacy_data.compressed()
        else:
            legacy_valid = legacy_data[np.isfinite(legacy_data)]

        # Filter sentinel values
        new_valid = new_valid[new_valid > -1.7e308]
        legacy_valid = legacy_valid[legacy_valid > -1.7e308]

        # Values should match
        assert len(new_valid) == len(
            legacy_valid
        ), f"Valid data count mismatch: new={len(new_valid)}, legacy={len(legacy_valid)}"

        assert np.allclose(
            np.sort(new_valid), np.sort(legacy_valid), rtol=1e-10, atol=1e-15
        ), "Data values differ between legacy format and legacy decoder"

    def test_bufr_metadata_equivalence(self, sample_bufr_file):
        """Test that legacy format metadata matches legacy decoder."""
        # Decode using new implementation
        # We need to pass legacy=True to get the same format as legacy implementation and have comparable outputs
        bufr_dict = bufr_to_dict(sample_bufr_file, logger_name="test_new_decoder", legacy=True)

        # Decode using legacy implementation
        path = "/".join(sample_bufr_file.split("/")[:-1]) + "/"
        filename = sample_bufr_file.split("/")[-1]
        vols = []
        run_logs = []
        legacy_bufr_to_dict(
            filename=filename,
            path=path,
            debug=False,
            volumenes=vols,
            run_logs=run_logs,
            logger_name="test_legacy_decoder",
        )
        bufr_dict_legacy = vols[0]

        new_info = bufr_dict["info"]  # type: ignore
        legacy_info = bufr_dict_legacy["info"]

        # Compare all core metadata fields
        for key in ["ano_vol", "mes_vol", "dia_vol", "hora_vol", "min_vol", "nsweeps"]:
            new_val = new_info.get(key)
            legacy_val = legacy_info.get(key)

            if new_val is not None and legacy_val is not None:
                assert new_val == legacy_val, f"Metadata {key} mismatch: new={new_val}, legacy={legacy_val}"

    def test_bufr_coordinates_equivalence(self, sample_bufr_file):
        """Test that geographic coordinates match."""
        # Decode using new implementation
        # We need to pass legacy=True to get the same format as legacy implementation and have comparable outputs
        bufr_dict = bufr_to_dict(sample_bufr_file, logger_name="test_new_decoder", legacy=True)

        # Decode using legacy implementation
        path = "/".join(sample_bufr_file.split("/")[:-1]) + "/"
        filename = sample_bufr_file.split("/")[-1]
        vols = []
        run_logs = []
        legacy_bufr_to_dict(
            filename=filename,
            path=path,
            debug=False,
            volumenes=vols,
            run_logs=run_logs,
            logger_name="test_legacy_decoder",
        )
        bufr_dict_legacy = vols[0]

        new_info = bufr_dict["info"]  # type: ignore
        legacy_info = bufr_dict_legacy["info"]

        tolerance = 1e-6

        for key in ["lat", "lon", "altura"]:
            new_val = new_info.get(key)
            legacy_val = legacy_info.get(key)

            if new_val is not None and legacy_val is not None:
                assert (
                    abs(new_val - legacy_val) < tolerance
                ), f"Coordinate {key} mismatch: new={new_val}, legacy={legacy_val}"


@pytest.mark.integration
@pytest.mark.filterwarnings("ignore:numpy.ndarray size changed.*:RuntimeWarning")
class TestyartConversion:
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
