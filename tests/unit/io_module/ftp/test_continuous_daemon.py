# -*- coding: utf-8 -*-
"""Tests for ContinuousDaemon."""

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from radarlib.io.ftp.continuous_daemon import ContinuousDaemon, ContinuousDaemonConfig, ContinuousDaemonError


@pytest.fixture
def temp_dirs(tmp_path):
    """Create temporary directories for testing."""
    bufr_dir = tmp_path / "bufr"
    bufr_dir.mkdir()
    state_db = tmp_path / "state.db"
    return bufr_dir, state_db


@pytest.fixture
def daemon_config(temp_dirs):
    """Create a test daemon configuration."""
    bufr_dir, state_db = temp_dirs
    return ContinuousDaemonConfig(
        host="ftp.example.com",
        username="testuser",
        password="testpass",
        radar_name="RMA1",
        remote_base_path="/L2",
        local_bufr_dir=bufr_dir,
        state_db=state_db,
        poll_interval=1,  # Short interval for testing
        start_date=datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc),
    )


class TestContinuousDaemonConfig:
    """Test suite for ContinuousDaemonConfig."""

    def test_config_creation(self, temp_dirs):
        """Test basic configuration creation."""
        bufr_dir, state_db = temp_dirs
        config = ContinuousDaemonConfig(
            host="ftp.example.com",
            username="user",
            password="pass",
            radar_name="RMA1",
            remote_base_path="/L2",
            local_bufr_dir=bufr_dir,
            state_db=state_db,
        )

        assert config.host == "ftp.example.com"
        assert config.username == "user"
        assert config.password == "pass"
        assert config.radar_name == "RMA1"
        assert config.remote_base_path == "/L2"
        assert config.local_bufr_dir == bufr_dir
        assert config.state_db == state_db
        assert config.poll_interval == 60  # Default value
        assert config.start_date is not None  # Auto-generated

    def test_config_with_custom_values(self, temp_dirs):
        """Test configuration with custom values."""
        bufr_dir, state_db = temp_dirs
        start_date = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
        vol_types = {"0315": {"01": ["DBZH", "DBZV"]}}

        config = ContinuousDaemonConfig(
            host="ftp.example.com",
            username="user",
            password="pass",
            radar_name="RMA2",
            remote_base_path="/L2",
            local_bufr_dir=bufr_dir,
            state_db=state_db,
            poll_interval=30,
            start_date=start_date,
            vol_types=vol_types,
            max_concurrent_downloads=10,
        )

        assert config.poll_interval == 30
        assert config.start_date == start_date
        assert config.vol_types == vol_types
        assert config.max_concurrent_downloads == 10


class TestContinuousDaemon:
    """Test suite for ContinuousDaemon."""

    def test_init(self, daemon_config):
        """Test daemon initialization."""
        with patch("radarlib.io.ftp.continuous_daemon.SQLiteStateTracker"):
            daemon = ContinuousDaemon(daemon_config)

            assert daemon.config == daemon_config
            assert daemon.radar_name == "RMA1"
            assert daemon.local_dir == daemon_config.local_bufr_dir
            assert daemon.poll_interval == daemon_config.poll_interval
            assert daemon._stats["bufr_files_downloaded"] == 0
            assert daemon._stats["bufr_files_failed"] == 0

    def test_init_creates_state_tracker(self, daemon_config):
        """Test that initialization creates state tracker."""
        with patch("radarlib.io.ftp.continuous_daemon.SQLiteStateTracker") as mock_tracker:
            daemon = ContinuousDaemon(daemon_config)

            mock_tracker.assert_called_once_with(daemon_config.state_db)
            assert daemon.state_tracker is not None

    def test_init_state_tracker_failure(self, daemon_config):
        """Test that initialization handles state tracker failure."""
        with patch(
            "radarlib.io.ftp.continuous_daemon.SQLiteStateTracker",
            side_effect=Exception("DB error"),
        ):
            with pytest.raises(ContinuousDaemonError, match="Failed to initialize state tracker"):
                ContinuousDaemon(daemon_config)

    def test_vol_types_setter_with_dict(self, daemon_config):
        """Test vol_types setter with dictionary."""
        with patch("radarlib.io.ftp.continuous_daemon.SQLiteStateTracker"):
            daemon = ContinuousDaemon(daemon_config)
            vol_types = {"0315": {"01": ["DBZH"]}}

            daemon.vol_types = vol_types

            # Should be converted to regex pattern
            assert daemon.vol_types is not None
            assert hasattr(daemon.vol_types, "pattern")

    def test_vol_types_setter_with_none(self, daemon_config):
        """Test vol_types setter with None."""
        with patch("radarlib.io.ftp.continuous_daemon.SQLiteStateTracker"):
            daemon = ContinuousDaemon(daemon_config)

            daemon.vol_types = None

            assert daemon._vol_types is None

    def test_new_bufr_files(self, daemon_config):
        """Test discovering new BUFR files."""
        with patch("radarlib.io.ftp.continuous_daemon.SQLiteStateTracker"):
            daemon = ContinuousDaemon(daemon_config)

            # Mock FTP client
            mock_client = MagicMock()
            mock_client.traverse_radar.return_value = [
                (datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc), "RMA1_0315_01_DBZH_20250101000000.BUFR", "/path1"),
                (datetime(2025, 1, 1, 0, 1, tzinfo=timezone.utc), "RMA1_0315_01_DBZV_20250101000100.BUFR", "/path2"),
            ]

            files = daemon.new_bufr_files(mock_client)

            assert len(files) == 2
            assert files[0][2] == "RMA1_0315_01_DBZH_20250101000000.BUFR"
            assert files[1][2] == "RMA1_0315_01_DBZV_20250101000100.BUFR"

    def test_stop(self, daemon_config):
        """Test stopping the daemon."""
        with patch("radarlib.io.ftp.continuous_daemon.SQLiteStateTracker"):
            daemon = ContinuousDaemon(daemon_config)
            daemon._running = True

            daemon.stop()

            assert daemon._running is False

    def test_get_stats(self, daemon_config):
        """Test getting daemon statistics."""
        with patch("radarlib.io.ftp.continuous_daemon.SQLiteStateTracker"):
            daemon = ContinuousDaemon(daemon_config)
            daemon._running = True
            daemon._stats["bufr_files_downloaded"] = 5
            daemon._stats["bufr_files_failed"] = 2

            stats = daemon.get_stats()

            assert stats["running"] is True
            assert stats["bufr_files_downloaded"] == 5
            assert stats["bufr_files_failed"] == 2

    @pytest.mark.asyncio
    async def test_run_service_resumes_from_last_download(self, daemon_config):
        """Test that run_service resumes from last downloaded file."""
        with patch("radarlib.io.ftp.continuous_daemon.SQLiteStateTracker") as mock_tracker_class:
            mock_tracker = MagicMock()
            mock_tracker_class.return_value = mock_tracker

            # Mock last downloaded file
            last_file = {
                "observation_datetime": "2025-01-02T00:00:00+00:00",
            }
            mock_tracker.get_latest_downloaded_file.return_value = last_file

            daemon = ContinuousDaemon(daemon_config)
            daemon._running = True

            # Mock the FTP client context manager
            mock_client = AsyncMock()
            mock_client.traverse_radar.return_value = []

            with patch("radarlib.io.ftp.continuous_daemon.RadarFTPClientAsync") as mock_client_class:
                mock_client_class.return_value.__aenter__.return_value = mock_client

                # Run one iteration then stop
                async def run_once():
                    # Create a task that will be cancelled
                    task = asyncio.create_task(daemon.run_service())
                    await asyncio.sleep(0.1)  # Let it run briefly
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

                await run_once()

            # Verify it checked for latest file
            mock_tracker.get_latest_downloaded_file.assert_called_with("RMA1")

    @pytest.mark.asyncio
    async def test_run_service_no_new_files(self, daemon_config):
        """Test run_service when no new files are found."""
        with patch("radarlib.io.ftp.continuous_daemon.SQLiteStateTracker") as mock_tracker_class:
            mock_tracker = MagicMock()
            mock_tracker_class.return_value = mock_tracker
            mock_tracker.get_latest_downloaded_file.return_value = None

            daemon = ContinuousDaemon(daemon_config)

            # Mock the FTP client to return no files
            mock_client = AsyncMock()
            mock_client.traverse_radar.return_value = []

            with patch("radarlib.io.ftp.continuous_daemon.RadarFTPClientAsync") as mock_client_class:
                mock_client_class.return_value.__aenter__.return_value = mock_client

                # Run one iteration
                async def run_once():
                    task = asyncio.create_task(daemon.run_service())
                    await asyncio.sleep(0.1)
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

                await run_once()

            # Should still connect to FTP
            assert mock_client_class.called
