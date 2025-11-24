# -*- coding: utf-8 -*-
"""Unit tests for DaemonManager."""

import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from radarlib.io.ftp.daemon_manager import DaemonManager, DaemonManagerConfig


class TestDaemonManagerConfig:
    """Tests for DaemonManagerConfig dataclass."""

    def test_config_creation(self):
        """Test creating a DaemonManagerConfig."""
        config = DaemonManagerConfig(
            radar_name="RMA1",
            base_path=Path("/base"),
            ftp_host="ftp.example.com",
            ftp_user="user",
            ftp_password="pass",
            ftp_base_path="/L2",
            volume_types={"0315": {"01": ["DBZH"]}},
            start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
        )

        assert config.radar_name == "RMA1"
        assert config.base_path == Path("/base")
        assert config.ftp_host == "ftp.example.com"
        assert config.enable_download_daemon is True
        assert config.enable_processing_daemon is True
        assert config.download_poll_interval == 60
        assert config.processing_poll_interval == 30

    def test_config_with_custom_values(self):
        """Test config with custom values."""
        config = DaemonManagerConfig(
            radar_name="RMA1",
            base_path=Path("/base"),
            ftp_host="ftp.example.com",
            ftp_user="user",
            ftp_password="pass",
            ftp_base_path="/L2",
            volume_types={"0315": {"01": ["DBZH"]}},
            start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
            download_poll_interval=120,
            processing_poll_interval=15,
            enable_download_daemon=False,
            enable_processing_daemon=True,
        )

        assert config.download_poll_interval == 120
        assert config.processing_poll_interval == 15
        assert config.enable_download_daemon is False
        assert config.enable_processing_daemon is True


class TestDaemonManager:
    """Tests for DaemonManager class."""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    @pytest.fixture
    def manager_config(self, temp_dir):
        """Create a test manager configuration."""
        return DaemonManagerConfig(
            radar_name="RMA1",
            base_path=temp_dir,
            ftp_host="ftp.example.com",
            ftp_user="user",
            ftp_password="pass",
            ftp_base_path="/L2",
            volume_types={"0315": {"01": ["DBZH"]}},
            start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
        )

    def test_init(self, manager_config):
        """Test manager initialization."""
        manager = DaemonManager(manager_config)

        assert manager.config == manager_config
        assert manager.download_daemon is None
        assert manager.processing_daemon is None
        assert manager._running is False
        assert manager._tasks == []

        # Check directories were created
        assert manager.bufr_dir.exists()
        assert manager.netcdf_dir.exists()
        assert manager.bufr_dir == manager_config.base_path / "bufr"
        assert manager.netcdf_dir == manager_config.base_path / "netcdf"

    def test_create_download_daemon(self, manager_config):
        """Test creating download daemon."""
        manager = DaemonManager(manager_config)
        daemon = manager._create_download_daemon()

        assert daemon is not None
        assert daemon.config.radar_name == "RMA1"
        assert daemon.config.host == "ftp.example.com"
        assert daemon.config.local_bufr_dir == manager.bufr_dir

    def test_create_processing_daemon(self, manager_config):
        """Test creating processing daemon."""
        manager = DaemonManager(manager_config)
        daemon = manager._create_processing_daemon()

        assert daemon is not None
        assert daemon.config.radar_name == "RMA1"
        assert daemon.config.local_bufr_dir == manager.bufr_dir
        assert daemon.config.local_netcdf_dir == manager.netcdf_dir

    def test_get_status_initial(self, manager_config):
        """Test getting initial status."""
        manager = DaemonManager(manager_config)
        status = manager.get_status()

        assert status["manager_running"] is False
        assert status["radar_code"] == "RMA1"
        assert status["download_daemon"]["enabled"] is True
        assert status["download_daemon"]["running"] is False
        assert status["processing_daemon"]["enabled"] is True
        assert status["processing_daemon"]["running"] is False

    def test_stop_when_not_running(self, manager_config):
        """Test stopping when not running."""
        manager = DaemonManager(manager_config)
        manager.stop()  # Should not raise any errors
        assert manager._running is False

    def test_update_config(self, manager_config):
        """Test updating configuration."""
        manager = DaemonManager(manager_config)

        manager.update_config(download_poll_interval=120, processing_poll_interval=15)

        assert manager.config.download_poll_interval == 120
        assert manager.config.processing_poll_interval == 15

    def test_update_config_unknown_param(self, manager_config):
        """Test updating with unknown parameter."""
        manager = DaemonManager(manager_config)

        # Should log warning but not raise error
        manager.update_config(unknown_param="value")

        # Original config unchanged
        assert not hasattr(manager.config, "unknown_param")

    def test_selective_daemon_download_only(self, temp_dir):
        """Test enabling only download daemon."""
        config = DaemonManagerConfig(
            radar_name="RMA1",
            base_path=temp_dir,
            ftp_host="ftp.example.com",
            ftp_user="user",
            ftp_password="pass",
            ftp_base_path="/L2",
            volume_types={"0315": {"01": ["DBZH"]}},
            start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
            enable_download_daemon=True,
            enable_processing_daemon=False,
        )

        manager = DaemonManager(config)
        status = manager.get_status()

        assert status["download_daemon"]["enabled"] is True
        assert status["processing_daemon"]["enabled"] is False

    def test_selective_daemon_processing_only(self, temp_dir):
        """Test enabling only processing daemon."""
        config = DaemonManagerConfig(
            radar_name="RMA1",
            base_path=temp_dir,
            ftp_host="ftp.example.com",
            ftp_user="user",
            ftp_password="pass",
            ftp_base_path="/L2",
            volume_types={"0315": {"01": ["DBZH"]}},
            start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
            enable_download_daemon=False,
            enable_processing_daemon=True,
        )

        manager = DaemonManager(config)
        status = manager.get_status()

        assert status["download_daemon"]["enabled"] is False
        assert status["processing_daemon"]["enabled"] is True

    def test_paths_setup(self, manager_config):
        """Test that paths are correctly set up."""
        manager = DaemonManager(manager_config)

        assert manager.bufr_dir == manager_config.base_path / "bufr"
        assert manager.netcdf_dir == manager_config.base_path / "netcdf"
        assert manager.state_db == manager_config.base_path / "state.db"
