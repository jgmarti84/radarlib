# -*- coding: utf-8 -*-
"""Tests for the daemons module using the new organization."""

from unittest.mock import patch

import pytest


class TestDaemonsImport:
    """Test that all daemon classes can be imported from the new location."""

    def test_import_download_daemon(self):
        """Test importing DownloadDaemon from new location."""
        from radarlib.daemons import DownloadDaemon, DownloadDaemonConfig, DownloadDaemonError

        assert DownloadDaemon is not None
        assert DownloadDaemonConfig is not None
        assert DownloadDaemonError is not None

    def test_import_processing_daemon(self):
        """Test importing ProcessingDaemon from new location."""
        from radarlib.daemons import ProcessingDaemon, ProcessingDaemonConfig

        assert ProcessingDaemon is not None
        assert ProcessingDaemonConfig is not None

    def test_import_product_daemon(self):
        """Test importing ProductGenerationDaemon from new location."""
        from radarlib.daemons import ProductGenerationDaemon, ProductGenerationDaemonConfig

        assert ProductGenerationDaemon is not None
        assert ProductGenerationDaemonConfig is not None

    def test_import_daemon_manager(self):
        """Test importing DaemonManager from new location."""
        from radarlib.daemons import DaemonManager, DaemonManagerConfig

        assert DaemonManager is not None
        assert DaemonManagerConfig is not None

    def test_backward_compatibility_aliases(self):
        """Test that backward compatibility aliases work."""
        from radarlib.daemons import (
            ContinuousDaemon,
            ContinuousDaemonConfig,
            ContinuousDaemonError,
            DownloadDaemon,
            DownloadDaemonConfig,
            DownloadDaemonError,
        )

        # New and old names should be the same class
        assert ContinuousDaemon is DownloadDaemon
        assert ContinuousDaemonConfig is DownloadDaemonConfig
        assert ContinuousDaemonError is DownloadDaemonError

    def test_legacy_daemons_import(self):
        """Test importing legacy daemons."""
        from radarlib.daemons import DateBasedDaemonConfig, DateBasedFTPDaemon, FTPDaemon, FTPDaemonConfig

        assert FTPDaemon is not None
        assert FTPDaemonConfig is not None
        assert DateBasedFTPDaemon is not None
        assert DateBasedDaemonConfig is not None


class TestDownloadDaemon:
    """Tests for DownloadDaemon using the new class name."""

    @pytest.fixture
    def temp_dirs(self, tmp_path):
        """Create temporary directories for testing."""
        bufr_dir = tmp_path / "bufr"
        bufr_dir.mkdir()
        state_db = tmp_path / "state.db"
        return bufr_dir, state_db

    def test_download_daemon_config(self, temp_dirs):
        """Test DownloadDaemonConfig creation."""
        from radarlib.daemons import DownloadDaemonConfig

        bufr_dir, state_db = temp_dirs
        config = DownloadDaemonConfig(
            host="ftp.example.com",
            username="user",
            password="pass",
            radar_name="RMA1",
            remote_base_path="/L2",
            local_bufr_dir=bufr_dir,
            state_db=state_db,
        )

        assert config.host == "ftp.example.com"
        assert config.radar_name == "RMA1"
        assert config.poll_interval == 60  # Default

    def test_download_daemon_init(self, temp_dirs):
        """Test DownloadDaemon initialization."""
        from radarlib.daemons import DownloadDaemon, DownloadDaemonConfig

        bufr_dir, state_db = temp_dirs
        config = DownloadDaemonConfig(
            host="ftp.example.com",
            username="user",
            password="pass",
            radar_name="RMA1",
            remote_base_path="/L2",
            local_bufr_dir=bufr_dir,
            state_db=state_db,
        )

        with patch("radarlib.daemons.download_daemon.SQLiteStateTracker"):
            daemon = DownloadDaemon(config)
            assert daemon.radar_name == "RMA1"
