"""Tests for volume filtering in DateBasedFTPDaemon."""

from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from radarlib.io.ftp import DateBasedDaemonConfig, DateBasedFTPDaemon


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

        return DateBasedDaemonConfig(
            host="ftp.example.com",
            username="user",
            password="pass",
            remote_base_path="/L2",
            radar_code="RMA1",
            local_download_dir=tmp_path / "downloads",
            state_db=tmp_path / "state.db",
            start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
            volume_types=volume_types,
        )

    @pytest.fixture
    def daemon_config_no_filtering(self, tmp_path):
        """Create daemon config without volume filtering."""
        return DateBasedDaemonConfig(
            host="ftp.example.com",
            username="user",
            password="pass",
            remote_base_path="/L2",
            radar_code="RMA1",
            local_download_dir=tmp_path / "downloads",
            state_db=tmp_path / "state.db",
            start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
            volume_types=None,  # No filtering
        )

    @patch("radarlib.io.ftp.date_daemon.FTPClient")
    def test_filter_accepts_valid_files(self, mock_client_class, daemon_config_with_filtering):
        """Test that valid files pass the filter."""
        daemon = DateBasedFTPDaemon(daemon_config_with_filtering)

        files = [
            "RMA1_0315_01_DBZH_20250101T120000Z.BUFR",  # Valid
            "RMA1_0315_01_ZDR_20250101T120000Z.BUFR",  # Valid
            "RMA1_0315_02_VRAD_20250101T120000Z.BUFR",  # Valid
        ]

        filtered = daemon._filter_files_by_volume(files)

        assert len(filtered) == 3
        assert all(f in filtered for f in files)

    @patch("radarlib.io.ftp.date_daemon.FTPClient")
    def test_filter_rejects_invalid_volume_code(self, mock_client_class, daemon_config_with_filtering):
        """Test that files with invalid volume code are rejected."""
        daemon = DateBasedFTPDaemon(daemon_config_with_filtering)

        files = [
            "RMA1_0315_01_DBZH_20250101T120000Z.BUFR",  # Valid
            "RMA1_9999_01_DBZH_20250101T120000Z.BUFR",  # Invalid vol code
        ]

        filtered = daemon._filter_files_by_volume(files)

        assert len(filtered) == 1
        assert "RMA1_0315_01_DBZH_20250101T120000Z.BUFR" in filtered
        assert "RMA1_9999_01_DBZH_20250101T120000Z.BUFR" not in filtered

    @patch("radarlib.io.ftp.date_daemon.FTPClient")
    def test_filter_rejects_invalid_volume_number(self, mock_client_class, daemon_config_with_filtering):
        """Test that files with invalid volume number are rejected."""
        daemon = DateBasedFTPDaemon(daemon_config_with_filtering)

        files = [
            "RMA1_0315_01_DBZH_20250101T120000Z.BUFR",  # Valid
            "RMA1_0315_99_DBZH_20250101T120000Z.BUFR",  # Invalid vol number
        ]

        filtered = daemon._filter_files_by_volume(files)

        assert len(filtered) == 1
        assert "RMA1_0315_01_DBZH_20250101T120000Z.BUFR" in filtered
        assert "RMA1_0315_99_DBZH_20250101T120000Z.BUFR" not in filtered

    @patch("radarlib.io.ftp.date_daemon.FTPClient")
    def test_filter_rejects_invalid_field_type(self, mock_client_class, daemon_config_with_filtering):
        """Test that files with invalid field type are rejected."""
        daemon = DateBasedFTPDaemon(daemon_config_with_filtering)

        files = [
            "RMA1_0315_01_DBZH_20250101T120000Z.BUFR",  # Valid
            "RMA1_0315_01_INVALID_20250101T120000Z.BUFR",  # Invalid field
        ]

        filtered = daemon._filter_files_by_volume(files)

        assert len(filtered) == 1
        assert "RMA1_0315_01_DBZH_20250101T120000Z.BUFR" in filtered
        assert "RMA1_0315_01_INVALID_20250101T120000Z.BUFR" not in filtered

    @patch("radarlib.io.ftp.date_daemon.FTPClient")
    def test_filter_handles_wrong_vol_number_for_field(self, mock_client_class, daemon_config_with_filtering):
        """Test that field types are validated per volume number."""
        daemon = DateBasedFTPDaemon(daemon_config_with_filtering)

        files = [
            "RMA1_0315_01_DBZH_20250101T120000Z.BUFR",  # Valid: DBZH in vol 01
            "RMA1_0315_02_VRAD_20250101T120000Z.BUFR",  # Valid: VRAD in vol 02
            "RMA1_0315_01_VRAD_20250101T120000Z.BUFR",  # Invalid: VRAD not in vol 01
            "RMA1_0315_02_DBZH_20250101T120000Z.BUFR",  # Invalid: DBZH not in vol 02
        ]

        filtered = daemon._filter_files_by_volume(files)

        assert len(filtered) == 2
        assert "RMA1_0315_01_DBZH_20250101T120000Z.BUFR" in filtered
        assert "RMA1_0315_02_VRAD_20250101T120000Z.BUFR" in filtered
        assert "RMA1_0315_01_VRAD_20250101T120000Z.BUFR" not in filtered
        assert "RMA1_0315_02_DBZH_20250101T120000Z.BUFR" not in filtered

    @patch("radarlib.io.ftp.date_daemon.FTPClient")
    def test_no_filtering_when_volume_types_none(self, mock_client_class, daemon_config_no_filtering):
        """Test that no filtering occurs when volume_types is None."""
        daemon = DateBasedFTPDaemon(daemon_config_no_filtering)

        files = [
            "RMA1_0315_01_DBZH_20250101T120000Z.BUFR",
            "RMA1_9999_99_INVALID_20250101T120000Z.BUFR",
            "RMA1_0000_00_TEST_20250101T120000Z.BUFR",
        ]

        filtered = daemon._filter_files_by_volume(files)

        # All files should pass when no filtering
        assert len(filtered) == 3
        assert all(f in filtered for f in files)

    @patch("radarlib.io.ftp.date_daemon.FTPClient")
    def test_filter_handles_malformed_filenames(self, mock_client_class, daemon_config_with_filtering):
        """Test that malformed filenames are skipped gracefully."""
        daemon = DateBasedFTPDaemon(daemon_config_with_filtering)

        files = [
            "RMA1_0315_01_DBZH_20250101T120000Z.BUFR",  # Valid
            "invalid_filename.BUFR",  # Malformed
            "RMA1_0315.BUFR",  # Too few parts
        ]

        filtered = daemon._filter_files_by_volume(files)

        assert len(filtered) == 1
        assert "RMA1_0315_01_DBZH_20250101T120000Z.BUFR" in filtered

    @patch("radarlib.io.ftp.date_daemon.FTPClient")
    def test_filter_multiple_volume_codes(self, mock_client_class, tmp_path):
        """Test filtering with multiple volume codes."""
        volume_types = {
            "0315": {"01": ["DBZH"], "02": ["VRAD"]},
            "0516": {"01": ["DBZV"], "02": ["WRAD"]},
        }

        config = DateBasedDaemonConfig(
            host="ftp.example.com",
            username="user",
            password="pass",
            remote_base_path="/L2",
            radar_code="RMA1",
            local_download_dir=tmp_path / "downloads",
            state_db=tmp_path / "state.db",
            start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
            volume_types=volume_types,
        )

        daemon = DateBasedFTPDaemon(config)

        files = [
            "RMA1_0315_01_DBZH_20250101T120000Z.BUFR",  # Valid
            "RMA1_0315_02_VRAD_20250101T120000Z.BUFR",  # Valid
            "RMA1_0516_01_DBZV_20250101T120000Z.BUFR",  # Valid
            "RMA1_0516_02_WRAD_20250101T120000Z.BUFR",  # Valid
            "RMA1_0315_01_VRAD_20250101T120000Z.BUFR",  # Invalid
            "RMA1_0516_01_DBZH_20250101T120000Z.BUFR",  # Invalid
        ]

        filtered = daemon._filter_files_by_volume(files)

        assert len(filtered) == 4
        assert "RMA1_0315_01_DBZH_20250101T120000Z.BUFR" in filtered
        assert "RMA1_0315_02_VRAD_20250101T120000Z.BUFR" in filtered
        assert "RMA1_0516_01_DBZV_20250101T120000Z.BUFR" in filtered
        assert "RMA1_0516_02_WRAD_20250101T120000Z.BUFR" in filtered
