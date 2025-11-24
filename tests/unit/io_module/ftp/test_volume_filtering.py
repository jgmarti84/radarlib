"""Tests for volume filtering in ContinuousDaemon."""

from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from radarlib.io.ftp import ContinuousDaemon, ContinuousDaemonConfig


class TestVolumeFiltering:
    """Test suite for volume filtering functionality."""

    @pytest.fixture
    def daemon_config_with_filtering(self, tmp_path):
        """Create daemon config with volume filtering."""
        volume_types = {
            "0315": {
                "01": ["DBZH", "DBZV", "ZDR"],
                "02": ["VRAD", "WRAD"],
            }
        }

        return ContinuousDaemonConfig(
            host="ftp.example.com",
            username="user",
            password="pass",
            remote_base_path="/L2",
            radar_name="RMA1",
            local_bufr_dir=tmp_path / "downloads",
            state_db=tmp_path / "state.db",
            start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
            vol_types=volume_types,
        )

    @pytest.fixture
    def daemon_config_no_filtering(self, tmp_path):
        """Create daemon config without volume filtering."""
        return ContinuousDaemonConfig(
            host="ftp.example.com",
            username="user",
            password="pass",
            remote_base_path="/L2",
            radar_name="RMA1",
            local_bufr_dir=tmp_path / "downloads",
            state_db=tmp_path / "state.db",
            start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
            vol_types=None,  # No filtering
        )

    @patch("radarlib.io.ftp.continuous_daemon.RadarFTPClientAsync")
    def test_filter_accepts_valid_files(self, mock_client_class, daemon_config_with_filtering):
        """Test that valid files pass the filter."""
        daemon = ContinuousDaemon(daemon_config_with_filtering)

        files = [
            "RMA1_0315_01_DBZH_20250101T120000Z.BUFR",  # Valid
            "RMA1_0315_01_ZDR_20250101T120000Z.BUFR",  # Valid
            "RMA1_0315_02_VRAD_20250101T120000Z.BUFR",  # Valid
        ]

        # Check that vol_types pattern accepts these files
        assert daemon.vol_types is not None, "vol_types should not be None"
        for filename in files:
            assert daemon.vol_types.match(filename), f"File {filename} should match vol_types pattern"

    @patch("radarlib.io.ftp.continuous_daemon.RadarFTPClientAsync")
    def test_filter_rejects_invalid_volume_code(self, mock_client_class, daemon_config_with_filtering):
        """Test that files with invalid volume code are rejected."""
        daemon = ContinuousDaemon(daemon_config_with_filtering)

        files = [
            "RMA1_0315_01_DBZH_20250101T120000Z.BUFR",  # Valid
            "RMA1_9999_01_DBZH_20250101T120000Z.BUFR",  # Invalid vol code
        ]

        # Check that vol_types pattern rejects invalid volume code
        assert daemon.vol_types is not None
        assert daemon.vol_types.match(files[0]), f"File {files[0]} should match"
        assert not daemon.vol_types.match(files[1]), f"File {files[1]} should not match invalid volume code"

    @patch("radarlib.io.ftp.continuous_daemon.RadarFTPClientAsync")
    def test_filter_rejects_invalid_volume_number(self, mock_client_class, daemon_config_with_filtering):
        """Test that files with invalid volume number are rejected."""
        daemon = ContinuousDaemon(daemon_config_with_filtering)

        files = [
            "RMA1_0315_01_DBZH_20250101T120000Z.BUFR",  # Valid
            "RMA1_0315_99_DBZH_20250101T120000Z.BUFR",  # Invalid vol number
        ]

        # Check that vol_types pattern rejects invalid volume number
        assert daemon.vol_types is not None
        assert daemon.vol_types.match(files[0]), f"File {files[0]} should match"
        assert not daemon.vol_types.match(files[1]), f"File {files[1]} should not match invalid volume number"

    @patch("radarlib.io.ftp.continuous_daemon.RadarFTPClientAsync")
    def test_filter_rejects_invalid_field_type(self, mock_client_class, daemon_config_with_filtering):
        """Test that files with invalid field type are rejected."""
        daemon = ContinuousDaemon(daemon_config_with_filtering)

        files = [
            "RMA1_0315_01_DBZH_20250101T120000Z.BUFR",  # Valid
            "RMA1_0315_01_INVALID_20250101T120000Z.BUFR",  # Invalid field
        ]

        # Check that vol_types pattern rejects invalid field type
        assert daemon.vol_types is not None
        assert daemon.vol_types.match(files[0]), f"File {files[0]} should match"
        assert not daemon.vol_types.match(files[1]), f"File {files[1]} should not match invalid field type"

    @patch("radarlib.io.ftp.continuous_daemon.RadarFTPClientAsync")
    def test_filter_handles_wrong_vol_number_for_field(self, mock_client_class, daemon_config_with_filtering):
        """Test that field types are validated per volume number."""
        daemon = ContinuousDaemon(daemon_config_with_filtering)

        files = [
            "RMA1_0315_01_DBZH_20250101T120000Z.BUFR",  # Valid: DBZH in vol 01
            "RMA1_0315_02_VRAD_20250101T120000Z.BUFR",  # Valid: VRAD in vol 02
            "RMA1_0315_01_VRAD_20250101T120000Z.BUFR",  # Invalid: VRAD not in vol 01
            "RMA1_0315_02_DBZH_20250101T120000Z.BUFR",  # Invalid: DBZH not in vol 02
        ]

        # Check that vol_types pattern validates field types per volume number
        assert daemon.vol_types is not None
        assert daemon.vol_types.match(files[0]), "DBZH should match in vol 01"
        assert daemon.vol_types.match(files[1]), "VRAD should match in vol 02"
        assert not daemon.vol_types.match(files[2]), "VRAD should not match in vol 01"
        assert not daemon.vol_types.match(files[3]), "DBZH should not match in vol 02"

    @patch("radarlib.io.ftp.continuous_daemon.RadarFTPClientAsync")
    def test_no_filtering_when_volume_types_none(self, mock_client_class, daemon_config_no_filtering):
        """Test that no filtering occurs when volume_types is None."""
        daemon = ContinuousDaemon(daemon_config_no_filtering)

        # When vol_types is None, all files should pass through (pattern is None)
        assert daemon.vol_types is None, "vol_types should be None when not specified"

    @patch("radarlib.io.ftp.continuous_daemon.RadarFTPClientAsync")
    def test_filter_handles_malformed_filenames(self, mock_client_class, daemon_config_with_filtering):
        """Test that malformed filenames are skipped gracefully."""
        daemon = ContinuousDaemon(daemon_config_with_filtering)

        files = [
            "RMA1_0315_01_DBZH_20250101T120000Z.BUFR",  # Valid
            "invalid_filename.BUFR",  # Malformed
            "RMA1_0315.BUFR",  # Too few parts
        ]

        # Check that vol_types pattern handles malformed filenames
        assert daemon.vol_types is not None
        assert daemon.vol_types.match(files[0]), "Valid file should match"
        assert not daemon.vol_types.match(files[1]), "Malformed filename should not match"
        assert not daemon.vol_types.match(files[2]), "Incomplete filename should not match"

    @patch("radarlib.io.ftp.continuous_daemon.RadarFTPClientAsync")
    def test_filter_multiple_volume_codes(self, mock_client_class, tmp_path):
        """Test filtering with multiple volume codes."""
        volume_types = {
            "0315": {"01": ["DBZH"], "02": ["VRAD"]},
            "0516": {"01": ["DBZV"], "02": ["WRAD"]},
        }

        config = ContinuousDaemonConfig(
            host="ftp.example.com",
            username="user",
            password="pass",
            remote_base_path="/L2",
            radar_name="RMA1",
            local_bufr_dir=tmp_path / "downloads",
            state_db=tmp_path / "state.db",
            start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
            vol_types=volume_types,
        )

        daemon = ContinuousDaemon(config)

        files = [
            "RMA1_0315_01_DBZH_20250101T120000Z.BUFR",  # Valid
            "RMA1_0315_02_VRAD_20250101T120000Z.BUFR",  # Valid
            "RMA1_0516_01_DBZV_20250101T120000Z.BUFR",  # Valid
            "RMA1_0516_02_WRAD_20250101T120000Z.BUFR",  # Valid
            "RMA1_0315_01_VRAD_20250101T120000Z.BUFR",  # Invalid
            "RMA1_0516_01_DBZH_20250101T120000Z.BUFR",  # Invalid
        ]

        # Check that vol_types pattern handles multiple volume codes correctly
        assert daemon.vol_types is not None
        assert daemon.vol_types.match(files[0]), "RMA1_0315_01_DBZH should match"
        assert daemon.vol_types.match(files[1]), "RMA1_0315_02_VRAD should match"
        assert daemon.vol_types.match(files[2]), "RMA1_0516_01_DBZV should match"
        assert daemon.vol_types.match(files[3]), "RMA1_0516_02_WRAD should match"
        assert not daemon.vol_types.match(files[4]), "RMA1_0315_01_VRAD should not match"
        assert not daemon.vol_types.match(files[5]), "RMA1_0516_01_DBZH should not match"
