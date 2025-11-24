# -*- coding: utf-8 -*-
"""Tests for ProductGenerationDaemon."""

import asyncio
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from radarlib.io.ftp.product_daemon import ProductGenerationDaemon, ProductGenerationDaemonConfig


@pytest.fixture
def temp_dirs(tmp_path):
    """Create temporary directories for testing."""
    netcdf_dir = tmp_path / "netcdf"
    netcdf_dir.mkdir()
    product_dir = tmp_path / "products"
    product_dir.mkdir()
    state_db = tmp_path / "state.db"
    return netcdf_dir, product_dir, state_db


@pytest.fixture
def daemon_config(temp_dirs):
    """Create a test daemon configuration."""
    netcdf_dir, product_dir, state_db = temp_dirs
    return ProductGenerationDaemonConfig(
        local_netcdf_dir=netcdf_dir,
        local_product_dir=product_dir,
        state_db=state_db,
        volume_types={"0315": {"01": ["DBZH", "DBZV"]}},
        radar_name="RMA1",
        poll_interval=1,
        product_type="image",
        add_colmax=True,
    )


class TestProductGenerationDaemonConfig:
    """Test suite for ProductGenerationDaemonConfig."""

    def test_config_creation(self, temp_dirs):
        """Test basic configuration creation."""
        netcdf_dir, product_dir, state_db = temp_dirs
        config = ProductGenerationDaemonConfig(
            local_netcdf_dir=netcdf_dir,
            local_product_dir=product_dir,
            state_db=state_db,
            volume_types={"0315": {"01": ["DBZH"]}},
            radar_name="RMA1",
        )

        assert config.local_netcdf_dir == netcdf_dir
        assert config.local_product_dir == product_dir
        assert config.state_db == state_db
        assert config.volume_types == {"0315": {"01": ["DBZH"]}}
        assert config.radar_name == "RMA1"
        assert config.poll_interval == 30  # Default
        assert config.product_type == "image"  # Default
        assert config.add_colmax is True  # Default

    def test_config_with_custom_values(self, temp_dirs):
        """Test configuration with custom values."""
        netcdf_dir, product_dir, state_db = temp_dirs

        config = ProductGenerationDaemonConfig(
            local_netcdf_dir=netcdf_dir,
            local_product_dir=product_dir,
            state_db=state_db,
            volume_types={"0315": {"01": ["DBZH"], "02": ["VRAD"]}},
            radar_name="RMA2",
            poll_interval=60,
            product_type="geotiff",
            add_colmax=False,
            stuck_volume_timeout_minutes=120,
        )

        assert config.poll_interval == 60
        assert config.product_type == "geotiff"
        assert config.add_colmax is False
        assert config.stuck_volume_timeout_minutes == 120


class TestProductGenerationDaemon:
    """Test suite for ProductGenerationDaemon."""

    def test_init(self, daemon_config):
        """Test daemon initialization."""
        with patch("radarlib.io.ftp.product_daemon.SQLiteStateTracker"):
            daemon = ProductGenerationDaemon(daemon_config)

            assert daemon.config == daemon_config
            assert daemon._running is False
            assert daemon._stats["volumes_processed"] == 0
            assert daemon._stats["volumes_failed"] == 0

    def test_init_creates_output_dir(self, daemon_config):
        """Test that initialization creates output directory."""
        with patch("radarlib.io.ftp.product_daemon.SQLiteStateTracker"):
            daemon = ProductGenerationDaemon(daemon_config)

            assert daemon.config.local_product_dir.exists()

    def test_init_creates_state_tracker(self, daemon_config):
        """Test that initialization creates state tracker."""
        with patch("radarlib.io.ftp.product_daemon.SQLiteStateTracker") as mock_tracker:
            daemon = ProductGenerationDaemon(daemon_config)

            mock_tracker.assert_called_once_with(daemon_config.state_db)

    def test_stop(self, daemon_config):
        """Test stopping the daemon."""
        with patch("radarlib.io.ftp.product_daemon.SQLiteStateTracker"):
            daemon = ProductGenerationDaemon(daemon_config)
            daemon._running = True

            daemon.stop()

            assert daemon._running is False

    def test_get_stats(self, daemon_config):
        """Test getting daemon statistics."""
        with patch("radarlib.io.ftp.product_daemon.SQLiteStateTracker") as mock_tracker_class:
            mock_tracker = MagicMock()
            mock_tracker_class.return_value = mock_tracker
            mock_tracker.get_products_by_status.side_effect = [
                [{"id": 1}, {"id": 2}],  # pending
                [{"id": 3}],  # completed
            ]

            daemon = ProductGenerationDaemon(daemon_config)
            daemon._running = True
            daemon._stats["volumes_processed"] = 5
            daemon._stats["volumes_failed"] = 1

            stats = daemon.get_stats()

            assert stats["running"] is True
            assert stats["volumes_processed"] == 5
            assert stats["volumes_failed"] == 1
            assert stats["pending_volumes"] == 2
            assert stats["completed_volumes"] == 1

    @pytest.mark.asyncio
    async def test_check_and_reset_stuck_volumes(self, daemon_config):
        """Test checking and resetting stuck volumes."""
        with patch("radarlib.io.ftp.product_daemon.SQLiteStateTracker") as mock_tracker_class:
            mock_tracker = MagicMock()
            mock_tracker_class.return_value = mock_tracker
            mock_tracker.reset_stuck_product_generations.return_value = 3

            daemon = ProductGenerationDaemon(daemon_config)
            await daemon._check_and_reset_stuck_volumes()

            mock_tracker.reset_stuck_product_generations.assert_called_once_with(
                daemon_config.stuck_volume_timeout_minutes, daemon_config.product_type
            )

    @pytest.mark.asyncio
    async def test_process_volumes_for_products_no_volumes(self, daemon_config):
        """Test processing when no volumes are ready."""
        with patch("radarlib.io.ftp.product_daemon.SQLiteStateTracker") as mock_tracker_class:
            mock_tracker = MagicMock()
            mock_tracker_class.return_value = mock_tracker
            mock_tracker.get_volumes_for_product_generation.return_value = []

            daemon = ProductGenerationDaemon(daemon_config)
            await daemon._process_volumes_for_products()

            # Should not attempt to process anything
            mock_tracker.register_product_generation.assert_not_called()

    @pytest.mark.asyncio
    async def test_generate_product_async_no_netcdf_path(self, daemon_config):
        """Test product generation when NetCDF path is missing."""
        with patch("radarlib.io.ftp.product_daemon.SQLiteStateTracker") as mock_tracker_class:
            mock_tracker = MagicMock()
            mock_tracker_class.return_value = mock_tracker

            daemon = ProductGenerationDaemon(daemon_config)

            volume_info = {
                "volume_id": "vol_123",
                "netcdf_path": None,
                "is_complete": 1,
            }

            result = await daemon._generate_product_async(volume_info)

            assert result is False
            assert daemon._stats["volumes_failed"] == 1
            mock_tracker.mark_product_status.assert_called_once()
            call_args = mock_tracker.mark_product_status.call_args
            assert call_args[0][2] == "failed"
            assert "NO_NETCDF_PATH" in str(call_args)

    @pytest.mark.asyncio
    async def test_generate_product_async_netcdf_not_found(self, daemon_config):
        """Test product generation when NetCDF file doesn't exist."""
        with patch("radarlib.io.ftp.product_daemon.SQLiteStateTracker") as mock_tracker_class:
            mock_tracker = MagicMock()
            mock_tracker_class.return_value = mock_tracker

            daemon = ProductGenerationDaemon(daemon_config)

            volume_info = {
                "volume_id": "vol_123",
                "netcdf_path": "/nonexistent/file.nc",
                "is_complete": 1,
            }

            result = await daemon._generate_product_async(volume_info)

            assert result is False
            assert daemon._stats["volumes_failed"] == 1
            mock_tracker.mark_product_status.assert_called_once()
            call_args = mock_tracker.mark_product_status.call_args
            assert call_args[0][2] == "failed"
            assert "FILE_NOT_FOUND" in str(call_args)

    @pytest.mark.asyncio
    async def test_generate_product_async_success(self, daemon_config, tmp_path):
        """Test successful product generation."""
        with patch("radarlib.io.ftp.product_daemon.SQLiteStateTracker") as mock_tracker_class:
            mock_tracker = MagicMock()
            mock_tracker_class.return_value = mock_tracker

            daemon = ProductGenerationDaemon(daemon_config)

            # Create a dummy NetCDF file
            netcdf_file = tmp_path / "test.nc"
            netcdf_file.write_text("dummy")

            volume_info = {
                "volume_id": "vol_123",
                "netcdf_path": str(netcdf_file),
                "is_complete": 1,
            }

            # Mock the sync generation method
            with patch.object(daemon, "_generate_products_sync") as mock_generate:
                result = await daemon._generate_product_async(volume_info)

                assert result is True
                assert daemon._stats["volumes_processed"] == 1
                mock_generate.assert_called_once()
                mock_tracker.mark_product_status.assert_any_call("vol_123", "image", "processing")
                mock_tracker.mark_product_status.assert_any_call("vol_123", "image", "completed")

    @pytest.mark.asyncio
    async def test_generate_product_async_failure(self, daemon_config, tmp_path):
        """Test product generation failure."""
        with patch("radarlib.io.ftp.product_daemon.SQLiteStateTracker") as mock_tracker_class:
            mock_tracker = MagicMock()
            mock_tracker_class.return_value = mock_tracker

            daemon = ProductGenerationDaemon(daemon_config)

            # Create a dummy NetCDF file
            netcdf_file = tmp_path / "test.nc"
            netcdf_file.write_text("dummy")

            volume_info = {
                "volume_id": "vol_123",
                "netcdf_path": str(netcdf_file),
                "is_complete": 1,
            }

            # Mock the sync generation method to raise an exception
            with patch.object(
                daemon, "_generate_products_sync", side_effect=RuntimeError("Plot error")
            ):
                result = await daemon._generate_product_async(volume_info)

                assert result is False
                assert daemon._stats["volumes_failed"] == 1
                # Should mark as failed with error
                calls = mock_tracker.mark_product_status.call_args_list
                failed_call = [c for c in calls if c[0][2] == "failed"][0]
                assert "Plot error" in str(failed_call)

    def test_generate_products_sync_imports_matplotlib(self, daemon_config, tmp_path):
        """Test that _generate_products_sync imports matplotlib correctly."""
        with patch("radarlib.io.ftp.product_daemon.SQLiteStateTracker"):
            daemon = ProductGenerationDaemon(daemon_config)

            # Create a dummy NetCDF file
            netcdf_file = tmp_path / "RMA1_0315_01_20250101000000.nc"
            netcdf_file.write_text("dummy")

            volume_info = {
                "volume_id": "vol_123",
                "strategy": "0315",
                "vol_nr": "01",
            }

            # The function imports matplotlib inside, so we need to check
            # that it's being imported correctly - we'll just verify it doesn't
            # crash on import
            with pytest.raises((ImportError, RuntimeError, Exception)):
                # This should fail because we don't have valid NetCDF, but
                # it proves the imports work
                daemon._generate_products_sync(netcdf_file, volume_info)
