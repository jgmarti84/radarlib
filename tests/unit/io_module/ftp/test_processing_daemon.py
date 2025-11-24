# -*- coding: utf-8 -*-
"""Tests for ProcessingDaemon."""

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from radarlib.io.ftp.processing_daemon import ProcessingDaemon, ProcessingDaemonConfig


@pytest.fixture
def temp_dirs(tmp_path):
    """Create temporary directories for testing."""
    bufr_dir = tmp_path / "bufr"
    bufr_dir.mkdir()
    netcdf_dir = tmp_path / "netcdf"
    netcdf_dir.mkdir()
    state_db = tmp_path / "state.db"
    return bufr_dir, netcdf_dir, state_db


@pytest.fixture
def daemon_config(temp_dirs):
    """Create a test daemon configuration."""
    bufr_dir, netcdf_dir, state_db = temp_dirs
    return ProcessingDaemonConfig(
        local_bufr_dir=bufr_dir,
        local_netcdf_dir=netcdf_dir,
        state_db=state_db,
        volume_types={"0315": {"01": ["DBZH", "DBZV"]}},
        radar_name="RMA1",
        poll_interval=1,
        max_concurrent_processing=2,
        start_date=datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc),
    )


class TestProcessingDaemonConfig:
    """Test suite for ProcessingDaemonConfig."""

    def test_config_creation(self, temp_dirs):
        """Test basic configuration creation."""
        bufr_dir, netcdf_dir, state_db = temp_dirs
        config = ProcessingDaemonConfig(
            local_bufr_dir=bufr_dir,
            local_netcdf_dir=netcdf_dir,
            state_db=state_db,
            volume_types={"0315": {"01": ["DBZH"]}},
            radar_name="RMA1",
        )

        assert config.local_bufr_dir == bufr_dir
        assert config.local_netcdf_dir == netcdf_dir
        assert config.state_db == state_db
        assert config.volume_types == {"0315": {"01": ["DBZH"]}}
        assert config.radar_name == "RMA1"
        assert config.poll_interval == 30  # Default
        assert config.max_concurrent_processing == 2  # Default

    def test_config_with_custom_values(self, temp_dirs):
        """Test configuration with custom values."""
        bufr_dir, netcdf_dir, state_db = temp_dirs
        resources = Path("/tmp/resources")

        config = ProcessingDaemonConfig(
            local_bufr_dir=bufr_dir,
            local_netcdf_dir=netcdf_dir,
            state_db=state_db,
            volume_types={"0315": {"01": ["DBZH"], "02": ["VRAD"]}},
            radar_name="RMA2",
            poll_interval=60,
            max_concurrent_processing=4,
            root_resources=resources,
            allow_incomplete=True,
            incomplete_timeout_hours=48,
            stuck_volume_timeout_minutes=120,
        )

        assert config.poll_interval == 60
        assert config.max_concurrent_processing == 4
        assert config.root_resources == resources
        assert config.allow_incomplete is True
        assert config.incomplete_timeout_hours == 48
        assert config.stuck_volume_timeout_minutes == 120

    def test_config_default_start_date(self, temp_dirs):
        """Test that start_date is auto-generated if not provided."""
        bufr_dir, netcdf_dir, state_db = temp_dirs
        config = ProcessingDaemonConfig(
            local_bufr_dir=bufr_dir,
            local_netcdf_dir=netcdf_dir,
            state_db=state_db,
            volume_types={"0315": {"01": ["DBZH"]}},
            radar_name="RMA1",
        )

        assert config.start_date is not None
        assert isinstance(config.start_date, datetime)


class TestProcessingDaemon:
    """Test suite for ProcessingDaemon."""

    def test_init(self, daemon_config):
        """Test daemon initialization."""
        with patch("radarlib.io.ftp.processing_daemon.SQLiteStateTracker"):
            daemon = ProcessingDaemon(daemon_config)

            assert daemon.config == daemon_config
            assert daemon._running is False
            assert daemon._processing_semaphore is None
            assert daemon._c_library_lock is None
            assert daemon._stats["volumes_processed"] == 0
            assert daemon._stats["volumes_failed"] == 0

    def test_init_creates_output_dir(self, daemon_config):
        """Test that initialization creates output directory."""
        with patch("radarlib.io.ftp.processing_daemon.SQLiteStateTracker"):
            daemon = ProcessingDaemon(daemon_config)

            assert daemon.config.local_netcdf_dir.exists()

    def test_init_creates_state_tracker(self, daemon_config):
        """Test that initialization creates state tracker."""
        with patch("radarlib.io.ftp.processing_daemon.SQLiteStateTracker") as mock_tracker:
            daemon = ProcessingDaemon(daemon_config)

            mock_tracker.assert_called_once_with(daemon_config.state_db)

    def test_stop(self, daemon_config):
        """Test stopping the daemon."""
        with patch("radarlib.io.ftp.processing_daemon.SQLiteStateTracker"):
            daemon = ProcessingDaemon(daemon_config)
            daemon._running = True

            daemon.stop()

            assert daemon._running is False

    def test_get_stats(self, daemon_config):
        """Test getting daemon statistics."""
        with patch("radarlib.io.ftp.processing_daemon.SQLiteStateTracker") as mock_tracker_class:
            mock_tracker = MagicMock()
            mock_tracker_class.return_value = mock_tracker
            mock_tracker.get_volumes_by_status.return_value = [{"id": 1}, {"id": 2}]
            mock_tracker.get_complete_unprocessed_volumes.return_value = [{"id": 1}]

            daemon = ProcessingDaemon(daemon_config)
            daemon._running = True
            daemon._stats["volumes_processed"] = 10
            daemon._stats["volumes_failed"] = 2
            daemon._stats["incomplete_volumes_detected"] = 3

            stats = daemon.get_stats()

            assert stats["running"] is True
            assert stats["volumes_processed"] == 10
            assert stats["volumes_failed"] == 2
            assert stats["incomplete_volumes_detected"] == 3
            assert stats["pending_volumes"] == 2
            assert stats["complete_unprocessed"] == 1

    @pytest.mark.asyncio
    async def test_check_and_reset_stuck_volumes(self, daemon_config):
        """Test checking and resetting stuck volumes."""
        with patch("radarlib.io.ftp.processing_daemon.SQLiteStateTracker") as mock_tracker_class:
            mock_tracker = MagicMock()
            mock_tracker_class.return_value = mock_tracker
            mock_tracker.reset_stuck_volumes.return_value = 2

            daemon = ProcessingDaemon(daemon_config)
            await daemon._check_and_reset_stuck_volumes()

            mock_tracker.reset_stuck_volumes.assert_called_once_with(
                daemon_config.stuck_volume_timeout_minutes
            )

    @pytest.mark.asyncio
    async def test_check_volume_completeness(self, daemon_config):
        """Test checking volume completeness."""
        with patch("radarlib.io.ftp.processing_daemon.SQLiteStateTracker") as mock_tracker_class:
            mock_tracker = MagicMock()
            mock_tracker_class.return_value = mock_tracker

            # Mock database operations
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            mock_tracker._get_connection.return_value = mock_conn

            # Mock query results
            mock_cursor.fetchall.return_value = [
                (
                    "RMA1_0315_01_DBZH_20250101000000.BUFR",
                    "RMA1",
                    "0315",
                    "01",
                    "DBZH",
                    "2025-01-01 00:00:00",
                    "/path/to/file.bufr",
                ),
                (
                    "RMA1_0315_01_DBZV_20250101000000.BUFR",
                    "RMA1",
                    "0315",
                    "01",
                    "DBZV",
                    "2025-01-01 00:00:00",
                    "/path/to/file2.bufr",
                ),
            ]

            mock_tracker.get_latest_registered_volume_datetime.return_value = None
            mock_tracker.get_volume_info.return_value = None
            mock_tracker.get_volume_id.return_value = "vol_123"

            daemon = ProcessingDaemon(daemon_config)
            await daemon._check_volume_completeness()

            # Should register the complete volume
            mock_tracker.register_volume.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_complete_volumes_no_volumes(self, daemon_config):
        """Test processing when no volumes are ready."""
        with patch("radarlib.io.ftp.processing_daemon.SQLiteStateTracker") as mock_tracker_class:
            mock_tracker = MagicMock()
            mock_tracker_class.return_value = mock_tracker
            mock_tracker.get_unprocessed_volumes.return_value = []

            daemon = ProcessingDaemon(daemon_config)
            await daemon._process_complete_volumes()

            # Should not attempt to process anything
            mock_tracker.mark_volume_processing.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_volume_async_success(self, daemon_config, tmp_path):
        """Test successful volume processing."""
        with patch("radarlib.io.ftp.processing_daemon.SQLiteStateTracker") as mock_tracker_class:
            mock_tracker = MagicMock()
            mock_tracker_class.return_value = mock_tracker

            # Create dummy BUFR file
            bufr_file = tmp_path / "test.bufr"
            bufr_file.write_text("dummy")

            mock_tracker.get_volume_files.return_value = [{"local_path": str(bufr_file)}]

            daemon = ProcessingDaemon(daemon_config)
            daemon._processing_semaphore = asyncio.Semaphore(1)
            daemon._c_library_lock = asyncio.Lock()

            volume_info = {
                "volume_id": "vol_123",
                "radar_name": "RMA1",
                "strategy": "0315",
                "vol_nr": "01",
                "observation_datetime": "2025-01-01 00:00:00",
                "is_complete": 1,
            }

            # Mock the decode and save method
            with patch.object(
                daemon, "_decode_and_save_volume", return_value=Path("/tmp/output.nc")
            ) as mock_decode:
                result = await daemon._process_volume_async(volume_info)

                assert result is True
                mock_tracker.mark_volume_processing.assert_any_call("vol_123", "processing")
                mock_tracker.mark_volume_processing.assert_any_call(
                    "vol_123", "completed", "/tmp/output.nc"
                )
                assert daemon._stats["volumes_processed"] == 1

    @pytest.mark.asyncio
    async def test_process_volume_async_failure(self, daemon_config):
        """Test volume processing failure."""
        with patch("radarlib.io.ftp.processing_daemon.SQLiteStateTracker") as mock_tracker_class:
            mock_tracker = MagicMock()
            mock_tracker_class.return_value = mock_tracker
            mock_tracker.get_volume_files.return_value = []  # No files

            daemon = ProcessingDaemon(daemon_config)
            daemon._processing_semaphore = asyncio.Semaphore(1)
            daemon._c_library_lock = asyncio.Lock()

            volume_info = {
                "volume_id": "vol_123",
                "radar_name": "RMA1",
                "strategy": "0315",
                "vol_nr": "01",
                "observation_datetime": "2025-01-01 00:00:00",
                "is_complete": 1,
            }

            result = await daemon._process_volume_async(volume_info)

            assert result is False
            assert daemon._stats["volumes_failed"] == 1
            # Should mark as failed with error message
            calls = [str(call) for call in mock_tracker.mark_volume_processing.call_args_list]
            assert any("failed" in call for call in calls)
