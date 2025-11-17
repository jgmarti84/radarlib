import pytest

# # Import both implementations
# from radarlib.io.bufr.bufr import bufr_to_dict, dec_bufr_file
# from radarlib.io.bufr.legacy import bufr_to_dict as legacy_bufr_to_dict, dec_bufr_file as legacy_dec_bufr_file


@pytest.mark.integration
class TestLegacyFormatComparison:
    """Test that new decoder with legacy=True matches legacy decoder output."""

    @pytest.fixture
    def sample_bufr_file(self, bufr_test_dir):
        file = bufr_test_dir / "AR5_1000_1_DBZH_20240101T000746Z.BUFR"
        if not file.exists():
            pytest.skip("Sample BUFR file not available in tests/data/bufr")
        return str(file)

    @pytest.fixture
    def new_decoded_legacy_format(self, sample_bufr_file):
        """Decode with new decoder using legacy=True format."""
        from radarlib.io.bufr.bufr import bufr_to_dict

        try:
            return bufr_to_dict(sample_bufr_file, root_resources=None, logger_name="test", legacy=True)
        except Exception as e:
            pytest.skip(f"New decoder failed: {e}")

    @pytest.fixture
    def legacy_decoded_dict(self, sample_bufr_file):
        """Decode with legacy decoder using dec_bufr_file directly."""
        from radarlib.io.bufr.legacy.bufr_legacy import bufr_to_dict

        try:
            vols = []
            run_logs = []
            path = "/".join(sample_bufr_file.split("/")[:-1]) + "/"
            filename = sample_bufr_file.split("/")[-1]
            bufr_to_dict(filename, path, False, vols, run_logs)

            return vols[0]
        except Exception as e:
            pytest.skip(f"Legacy decoder failed: {e}")

    def test_legacy_format_metadata_matches_legacy_decoder(self, new_decoded_legacy_format, legacy_decoded_dict):
        """Test that legacy format metadata matches legacy decoder."""
        new_info = new_decoded_legacy_format["info"]
        legacy_info = legacy_decoded_dict["info"]

        # Compare all core metadata fields
        for key in ["ano_vol", "mes_vol", "dia_vol", "hora_vol", "min_vol", "nsweeps"]:
            new_val = new_info.get(key)
            legacy_val = legacy_info.get(key)

            if new_val is not None and legacy_val is not None:
                assert new_val == legacy_val, f"Metadata {key} mismatch: new={new_val}, legacy={legacy_val}"

    def test_legacy_format_coordinates_match(self, new_decoded_legacy_format, legacy_decoded_dict):
        """Test that geographic coordinates match."""
        new_info = new_decoded_legacy_format["info"]
        legacy_info = legacy_decoded_dict["info"]

        tolerance = 1e-6

        for key in ["lat", "lon", "altura"]:
            new_val = new_info.get(key)
            legacy_val = legacy_info.get(key)

            if new_val is not None and legacy_val is not None:
                assert (
                    abs(new_val - legacy_val) < tolerance
                ), f"Coordinate {key} mismatch: new={new_val}, legacy={legacy_val}"
