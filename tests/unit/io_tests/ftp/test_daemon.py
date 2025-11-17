"""Tests for FTP daemon."""

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from radarlib.io.ftp import FTPDaemon, FTPDaemonConfig


@pytest.fixture
def daemon_config(tmp_path):
    """Create a test daemon configuration."""
    return FTPDaemonConfig(
        host="ftp.example.com",
        username="user",
        password="pass",
        remote_base_path="/L2",
        local_download_dir=tmp_path / "downloads",
        state_file=tmp_path / "state.json",
        poll_interval=1,  # Short interval for testing
        max_concurrent_downloads=2
    )


class TestFTPDaemonConfig:
    """Test suite for FTPDaemonConfig dataclass."""

    def test_config_creation(self, tmp_path):
        """Test creating daemon config."""
        config = FTPDaemonConfig(
            host="ftp.example.com",
            username="user",
            password="pass",
            remote_base_path="/L2",
            local_download_dir=tmp_path / "downloads",
            state_file=tmp_path / "state.json"
        )

        assert config.host == "ftp.example.com"
        assert config.username == "user"
        assert config.poll_interval == 60  # Default
        assert config.max_concurrent_downloads == 5  # Default


class TestFTPDaemon:
    """Test suite for FTPDaemon class."""

    def test_init(self, daemon_config):
        """Test daemon initialization."""
        daemon = FTPDaemon(daemon_config)

        assert daemon.config == daemon_config
        assert daemon.client is not None
        assert daemon.state_tracker is not None
        assert daemon._running is False

    def test_init_creates_download_dir(self, daemon_config):
        """Test that daemon creates download directory."""
        daemon = FTPDaemon(daemon_config)

        assert daemon_config.local_download_dir.exists()

    @pytest.mark.asyncio
    @patch("radarlib.io.ftp.daemon.FTPClient")
    async def test_discover_new_files(self, mock_client_class, daemon_config):
        """Test discovering new files."""
        # Setup mocks
        mock_client = MagicMock()
        mock_client.list_files.return_value = [
            "file1.BUFR",
            "file2.BUFR",
            "file3.txt"  # Not a BUFR file
        ]
        mock_client_class.return_value = mock_client

        daemon = FTPDaemon(daemon_config)
        daemon.client = mock_client

        # Discover files
        new_files = await daemon._discover_new_files()

        # Verify only BUFR files are returned
        assert len(new_files) == 2
        assert "/L2/file1.BUFR" in new_files
        assert "/L2/file2.BUFR" in new_files

    @pytest.mark.asyncio
    @patch("radarlib.io.ftp.daemon.FTPClient")
    async def test_discover_new_files_filters_downloaded(self, mock_client_class, daemon_config):
        """Test that already downloaded files are filtered out."""
        # Setup mocks
        mock_client = MagicMock()
        mock_client.list_files.return_value = ["file1.BUFR", "file2.BUFR"]
        mock_client_class.return_value = mock_client

        daemon = FTPDaemon(daemon_config)
        daemon.client = mock_client

        # Mark file1 as already downloaded
        daemon.state_tracker.mark_downloaded("file1.BUFR", "/L2/file1.BUFR")

        # Discover files
        new_files = await daemon._discover_new_files()

        # Verify only file2 is returned
        assert len(new_files) == 1
        assert "/L2/file2.BUFR" in new_files

    @pytest.mark.asyncio
    @patch("radarlib.io.ftp.daemon.FTPClient")
    async def test_download_file_async_success(self, mock_client_class, daemon_config):
        """Test successful async file download."""
        # Setup mocks
        mock_client = MagicMock()
        mock_client.download_file = MagicMock()
        mock_client_class.return_value = mock_client

        daemon = FTPDaemon(daemon_config)
        daemon.client = mock_client
        daemon._download_semaphore = asyncio.Semaphore(2)

        # Download file
        result = await daemon._download_file_async("/L2/file1.BUFR")

        assert result is True
        assert daemon.state_tracker.is_downloaded("file1.BUFR")

    @pytest.mark.asyncio
    @patch("radarlib.io.ftp.daemon.FTPClient")
    async def test_download_file_async_failure(self, mock_client_class, daemon_config):
        """Test failed async file download."""
        # Setup mocks
        mock_client = MagicMock()
        mock_client.download_file.side_effect = Exception("Download failed")
        mock_client_class.return_value = mock_client

        daemon = FTPDaemon(daemon_config)
        daemon.client = mock_client
        daemon._download_semaphore = asyncio.Semaphore(2)

        # Download file
        result = await daemon._download_file_async("/L2/file1.BUFR")

        assert result is False
        assert not daemon.state_tracker.is_downloaded("file1.BUFR")

    @pytest.mark.asyncio
    @patch("radarlib.io.ftp.daemon.FTPClient")
    async def test_check_and_download_new_files(self, mock_client_class, daemon_config):
        """Test check and download cycle."""
        # Setup mocks
        mock_client = MagicMock()
        mock_client.list_files.return_value = ["file1.BUFR"]
        mock_client.download_file = MagicMock()
        mock_client_class.return_value = mock_client

        daemon = FTPDaemon(daemon_config)
        daemon.client = mock_client
        daemon._download_semaphore = asyncio.Semaphore(2)

        # Run check cycle
        await daemon._check_and_download_new_files()

        # Verify file was downloaded
        assert daemon.state_tracker.is_downloaded("file1.BUFR")

    @pytest.mark.asyncio
    @patch("radarlib.io.ftp.daemon.FTPClient")
    async def test_check_and_download_no_new_files(self, mock_client_class, daemon_config):
        """Test check cycle with no new files."""
        # Setup mocks
        mock_client = MagicMock()
        mock_client.list_files.return_value = []
        mock_client_class.return_value = mock_client

        daemon = FTPDaemon(daemon_config)
        daemon.client = mock_client

        # Run check cycle - should not error
        await daemon._check_and_download_new_files()

        assert daemon.state_tracker.count() == 0

    def test_get_stats(self, daemon_config):
        """Test getting daemon statistics."""
        daemon = FTPDaemon(daemon_config)
        daemon.state_tracker.mark_downloaded("file1.BUFR", "/L2/file1.BUFR")

        stats = daemon.get_stats()

        assert stats["running"] is False
        assert stats["total_downloaded"] == 1
        assert stats["config"]["host"] == "ftp.example.com"

    def test_stop(self, daemon_config):
        """Test stopping daemon."""
        daemon = FTPDaemon(daemon_config)
        daemon._running = True

        daemon.stop()

        assert daemon._running is False

    @pytest.mark.asyncio
    @patch("radarlib.io.ftp.daemon.FTPClient")
    async def test_run_stops_on_keyboard_interrupt(self, mock_client_class, daemon_config):
        """Test that daemon stops gracefully on KeyboardInterrupt."""
        # Setup mocks
        mock_client = MagicMock()
        mock_client.list_files.side_effect = KeyboardInterrupt()
        mock_client_class.return_value = mock_client

        daemon = FTPDaemon(daemon_config)
        daemon.client = mock_client

        # Run should handle KeyboardInterrupt gracefully
        await daemon.run()

        assert daemon._running is False
