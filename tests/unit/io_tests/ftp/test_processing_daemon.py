# -*- coding: utf-8 -*-
"""Unit tests for ProcessingDaemon."""

import tempfile
from pathlib import Path

import pytest

from radarlib.io.ftp.processing_daemon import ProcessingDaemon, ProcessingDaemonConfig


class TestProcessingDaemonConfig:
    """Tests for ProcessingDaemonConfig dataclass."""

    def test_config_creation(self):
        """Test creating a ProcessingDaemonConfig."""
        config = ProcessingDaemonConfig(
            local_bufr_dir=Path("/bufr"),
            local_netcdf_dir=Path("/netcdf"),
            state_db=Path("/state.db"),
            volume_types={"0315": {"01": ["DBZH", "DBZV"]}},
            radar_code="RMA1",
        )

        assert config.local_bufr_dir == Path("/bufr")
        assert config.local_netcdf_dir == Path("/netcdf")
        assert config.state_db == Path("/state.db")
        assert config.radar_code == "RMA1"
        assert config.poll_interval == 30  # Default
        assert config.max_concurrent_processing == 2  # Default

    def test_config_with_custom_values(self):
        """Test config with custom values."""
        config = ProcessingDaemonConfig(
            local_bufr_dir=Path("/bufr"),
            local_netcdf_dir=Path("/netcdf"),
            state_db=Path("/state.db"),
            volume_types={"0315": {"01": ["DBZH"]}},
            radar_code="RMA1",
            poll_interval=60,
            max_concurrent_processing=4,
            allow_incomplete=True,
        )

        assert config.poll_interval == 60
        assert config.max_concurrent_processing == 4
        assert config.allow_incomplete is True


class TestProcessingDaemon:
    """Tests for ProcessingDaemon class."""

    @pytest.fixture
    def temp_dirs(self):
        """Create temporary directories for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            dirs = {
                "bufr": base / "bufr",
                "netcdf": base / "netcdf",
                "db": base / "state.db",
            }
            dirs["bufr"].mkdir(parents=True, exist_ok=True)
            yield dirs

    @pytest.fixture
    def daemon_config(self, temp_dirs):
        """Create a test daemon configuration."""
        return ProcessingDaemonConfig(
            local_bufr_dir=temp_dirs["bufr"],
            local_netcdf_dir=temp_dirs["netcdf"],
            state_db=temp_dirs["db"],
            volume_types={
                "0315": {
                    "01": ["DBZH", "DBZV"],
                    "02": ["VRAD", "WRAD"],
                }
            },
            radar_code="RMA1",
            poll_interval=1,  # Short interval for testing
        )

    def test_init(self, daemon_config):
        """Test daemon initialization."""
        daemon = ProcessingDaemon(daemon_config)

        assert daemon.config == daemon_config
        assert daemon.state_tracker is not None
        assert daemon._running is False
        assert daemon.config.local_netcdf_dir.exists()

    def test_get_stats_initial(self, daemon_config):
        """Test getting initial statistics."""
        daemon = ProcessingDaemon(daemon_config)
        stats = daemon.get_stats()

        assert stats["running"] is False
        assert stats["volumes_processed"] == 0
        assert stats["volumes_failed"] == 0
        assert stats["incomplete_volumes_detected"] == 0

    def test_stop(self, daemon_config):
        """Test stopping the daemon."""
        daemon = ProcessingDaemon(daemon_config)
        daemon._running = True

        daemon.stop()
        assert daemon._running is False

    @pytest.mark.asyncio
    async def test_check_volume_completeness_empty(self, daemon_config):
        """Test volume completeness check with no files."""
        daemon = ProcessingDaemon(daemon_config)
        await daemon._check_volume_completeness()

        # Should not create any volumes
        volumes = daemon.state_tracker.get_volumes_by_status("pending")
        assert len(volumes) == 0

    @pytest.mark.asyncio
    async def test_check_volume_completeness_incomplete(self, daemon_config):
        """Test detecting incomplete volume."""
        daemon = ProcessingDaemon(daemon_config)

        # Add one file from a volume that needs two fields
        daemon.state_tracker.mark_downloaded(
            "RMA1_0315_01_DBZH_20251118T123000Z.BUFR",
            "/remote/path",
            str(daemon_config.local_bufr_dir / "RMA1_0315_01_DBZH_20251118T123000Z.BUFR"),
            1000,
            "abc123",
            {
                "radar_code": "RMA1",
                "field_type": "DBZH",
                "observation_datetime": "2025-11-18T12:30:00Z",
            },
        )

        await daemon._check_volume_completeness()

        # Should detect incomplete volume
        volumes = daemon.state_tracker.get_complete_unprocessed_volumes()
        assert len(volumes) == 0

        # But should be registered
        all_volumes = daemon.state_tracker.get_volumes_by_status("pending")
        assert len(all_volumes) == 1
        assert all_volumes[0]["is_complete"] == 0

    @pytest.mark.asyncio
    async def test_check_volume_completeness_complete(self, daemon_config):
        """Test detecting complete volume."""
        daemon = ProcessingDaemon(daemon_config)

        # Add all required files for volume
        files = [
            ("RMA1_0315_01_DBZH_20251118T123000Z.BUFR", "DBZH"),
            ("RMA1_0315_01_DBZV_20251118T123000Z.BUFR", "DBZV"),
        ]

        for filename, field_type in files:
            daemon.state_tracker.mark_downloaded(
                filename,
                "/remote/path",
                str(daemon_config.local_bufr_dir / filename),
                1000,
                "abc123",
                {
                    "radar_code": "RMA1",
                    "field_type": field_type,
                    "observation_datetime": "2025-11-18T12:30:00Z",
                },
            )

        await daemon._check_volume_completeness()

        # Should detect complete volume
        volumes = daemon.state_tracker.get_complete_unprocessed_volumes()
        assert len(volumes) == 1
        assert volumes[0]["is_complete"] == 1
        assert volumes[0]["vol_code"] == "0315"
        assert volumes[0]["vol_number"] == "01"

    @pytest.mark.asyncio
    async def test_check_volume_completeness_multiple_volumes(self, daemon_config):
        """Test detecting multiple complete volumes."""
        daemon = ProcessingDaemon(daemon_config)

        # Add files for two different volumes
        volumes_data = [
            ("0315", "01", ["DBZH", "DBZV"], "2025-11-18T12:00:00Z"),
            ("0315", "02", ["VRAD", "WRAD"], "2025-11-18T12:30:00Z"),
        ]

        for vol_code, vol_num, fields, timestamp in volumes_data:
            for field_type in fields:
                filename = f"RMA1_{vol_code}_{vol_num}_{field_type}_{timestamp.replace(':', '').replace('-', '')}Z.BUFR"
                daemon.state_tracker.mark_downloaded(
                    filename,
                    "/remote/path",
                    str(daemon_config.local_bufr_dir / filename),
                    1000,
                    "abc123",
                    {
                        "radar_code": "RMA1",
                        "field_type": field_type,
                        "observation_datetime": timestamp,
                    },
                )

        await daemon._check_volume_completeness()

        # Should detect both complete volumes
        volumes = daemon.state_tracker.get_complete_unprocessed_volumes()
        assert len(volumes) == 2

    @pytest.mark.asyncio
    async def test_process_complete_volumes_no_volumes(self, daemon_config):
        """Test processing when no complete volumes exist."""
        daemon = ProcessingDaemon(daemon_config)
        daemon._processing_semaphore = daemon_config.max_concurrent_processing

        await daemon._process_complete_volumes()

        # Should do nothing
        stats = daemon.get_stats()
        assert stats["volumes_processed"] == 0

    @pytest.mark.asyncio
    async def test_decode_and_save_volume_no_files(self, daemon_config):
        """Test decoding fails gracefully with no files."""
        daemon = ProcessingDaemon(daemon_config)

        with pytest.raises(ValueError, match="No BUFR files could be decoded"):
            daemon._decode_and_save_volume([], "test_volume", "RMA1")

    def test_get_stats_after_processing(self, daemon_config):
        """Test statistics after simulated processing."""
        daemon = ProcessingDaemon(daemon_config)

        # Simulate some processing
        daemon._stats["volumes_processed"] = 5
        daemon._stats["volumes_failed"] = 2
        daemon._stats["incomplete_volumes_detected"] = 3

        stats = daemon.get_stats()
        assert stats["volumes_processed"] == 5
        assert stats["volumes_failed"] == 2
        assert stats["incomplete_volumes_detected"] == 3

    @pytest.mark.asyncio
    async def test_check_volume_completeness_filters_wrong_radar(self, daemon_config):
        """Test that volumes for other radars are ignored."""
        daemon = ProcessingDaemon(daemon_config)

        # Add file for different radar
        daemon.state_tracker.mark_downloaded(
            "RMA5_0315_01_DBZH_20251118T123000Z.BUFR",
            "/remote/path",
            str(daemon_config.local_bufr_dir / "RMA5_0315_01_DBZH_20251118T123000Z.BUFR"),
            1000,
            "abc123",
            {
                "radar_code": "RMA5",  # Different radar
                "field_type": "DBZH",
                "observation_datetime": "2025-11-18T12:30:00Z",
            },
        )

        await daemon._check_volume_completeness()

        # Should not create any volumes for RMA1
        volumes = daemon.state_tracker.get_volumes_by_status("pending")
        assert len(volumes) == 0

    @pytest.mark.asyncio
    async def test_check_volume_completeness_filters_unconfigured_volume(self, daemon_config):
        """Test that unconfigured volume types are ignored."""
        daemon = ProcessingDaemon(daemon_config)

        # Add file for volume type not in config
        daemon.state_tracker.mark_downloaded(
            "RMA1_9999_01_DBZH_20251118T123000Z.BUFR",
            "/remote/path",
            str(daemon_config.local_bufr_dir / "RMA1_9999_01_DBZH_20251118T123000Z.BUFR"),
            1000,
            "abc123",
            {
                "radar_code": "RMA1",
                "field_type": "DBZH",
                "observation_datetime": "2025-11-18T12:30:00Z",
            },
        )

        await daemon._check_volume_completeness()

        # Should not create any volumes
        volumes = daemon.state_tracker.get_volumes_by_status("pending")
        assert len(volumes) == 0
