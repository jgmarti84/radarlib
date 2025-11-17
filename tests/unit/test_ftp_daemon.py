"""
Unit tests for the FTP daemon service.

These tests use mocking to avoid requiring an actual FTP server.
"""

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from radarlib.io.ftp.daemon import FTPDaemon, FTPDaemonConfig


@pytest.fixture
def daemon_config(tmp_path):
    """Create a test daemon configuration."""
    return FTPDaemonConfig(
        host="ftp.example.com",
        username="testuser",
        password="testpass",
        remote_path="/radar/data",
        local_dir=str(tmp_path / "downloads"),
        poll_interval=1,  # Short interval for testing
        max_concurrent_downloads=2,
    )


@pytest.fixture
def mock_ftp_client():
    """Create a mock AsyncFTPClient."""
    mock_client = MagicMock()
    mock_client.connect = AsyncMock()
    mock_client.disconnect = AsyncMock()
    mock_client.list_files = AsyncMock(return_value=[])
    mock_client.download_files = AsyncMock(return_value=[])
    return mock_client


@pytest.mark.asyncio
async def test_daemon_initialization(daemon_config, tmp_path):
    """Test daemon initialization."""
    daemon = FTPDaemon(daemon_config)
    await daemon.initialize()

    assert daemon.config == daemon_config
    assert daemon._running is False
    assert daemon._processed_files == set()


@pytest.mark.asyncio
async def test_daemon_initialization_with_existing_files(daemon_config, tmp_path):
    """Test daemon initialization with existing local files."""
    # Create some existing files
    local_dir = Path(daemon_config.local_dir)
    local_dir.mkdir(parents=True, exist_ok=True)
    (local_dir / "file1.BUFR").touch()
    (local_dir / "file2.BUFR").touch()

    daemon = FTPDaemon(daemon_config)
    await daemon.initialize()

    assert len(daemon._processed_files) == 2
    assert "file1.BUFR" in daemon._processed_files
    assert "file2.BUFR" in daemon._processed_files


@pytest.mark.asyncio
async def test_daemon_run_once_no_files(daemon_config, mock_ftp_client):
    """Test daemon run_once with no new files."""
    with patch("radarlib.io.ftp.daemon.AsyncFTPClient", return_value=mock_ftp_client):
        daemon = FTPDaemon(daemon_config)
        count = await daemon.run_once()

        assert count == 0
        mock_ftp_client.connect.assert_called_once()
        mock_ftp_client.disconnect.assert_called_once()


@pytest.mark.asyncio
async def test_daemon_run_once_with_new_files(daemon_config, mock_ftp_client, tmp_path):
    """Test daemon run_once with new files to download."""
    # Mock file listing and downloads
    remote_files = ["/radar/data/file1.BUFR", "/radar/data/file2.BUFR"]
    local_dir = Path(daemon_config.local_dir)

    mock_ftp_client.list_files = AsyncMock(return_value=remote_files)
    mock_ftp_client.download_files = AsyncMock(
        return_value=[str(local_dir / "file1.BUFR"), str(local_dir / "file2.BUFR")]
    )

    with patch("radarlib.io.ftp.daemon.AsyncFTPClient", return_value=mock_ftp_client):
        daemon = FTPDaemon(daemon_config)
        count = await daemon.run_once()

        assert count == 2
        assert "file1.BUFR" in daemon._processed_files
        assert "file2.BUFR" in daemon._processed_files
        mock_ftp_client.download_files.assert_called_once()


@pytest.mark.asyncio
async def test_daemon_callback_on_download(daemon_config, mock_ftp_client, tmp_path):
    """Test that callback is called when files are downloaded."""
    callback_called = []

    def test_callback(local_path: str):
        callback_called.append(local_path)

    # Mock file listing and downloads
    local_dir = Path(daemon_config.local_dir)
    remote_files = ["/radar/data/file1.BUFR"]
    downloaded_files = [str(local_dir / "file1.BUFR")]

    mock_ftp_client.list_files = AsyncMock(return_value=remote_files)
    mock_ftp_client.download_files = AsyncMock(return_value=downloaded_files)

    with patch("radarlib.io.ftp.daemon.AsyncFTPClient", return_value=mock_ftp_client):
        daemon = FTPDaemon(daemon_config, on_file_downloaded=test_callback)
        await daemon.run_once()

        assert len(callback_called) == 1
        assert callback_called[0] == downloaded_files[0]


@pytest.mark.asyncio
async def test_daemon_ignores_already_processed_files(daemon_config, mock_ftp_client):
    """Test that daemon doesn't re-download already processed files."""
    # Setup: first run downloads files
    remote_files = ["/radar/data/file1.BUFR", "/radar/data/file2.BUFR"]
    local_dir = Path(daemon_config.local_dir)

    mock_ftp_client.list_files = AsyncMock(return_value=remote_files)

    # First call to download_files returns successfully downloaded files
    first_download = AsyncMock(return_value=[str(local_dir / "file1.BUFR"), str(local_dir / "file2.BUFR")])
    mock_ftp_client.download_files = first_download

    with patch("radarlib.io.ftp.daemon.AsyncFTPClient", return_value=mock_ftp_client):
        daemon = FTPDaemon(daemon_config)

        # First run - should download all files
        count1 = await daemon.run_once()
        assert count1 == 2
        assert first_download.call_count == 1

        # Second run - should not download any files (already processed)
        # download_files should not be called at all since there are no new files
        second_download = AsyncMock()
        mock_ftp_client.download_files = second_download

        count2 = await daemon.run_once()
        assert count2 == 0
        # Verify download_files was NOT called since no new files were found
        assert second_download.call_count == 0


@pytest.mark.asyncio
async def test_daemon_run_with_max_iterations(daemon_config, mock_ftp_client):
    """Test daemon run with maximum iterations."""
    with patch("radarlib.io.ftp.daemon.AsyncFTPClient", return_value=mock_ftp_client):
        daemon = FTPDaemon(daemon_config)

        # Run for 2 iterations
        await daemon.run(max_iterations=2)

        # Should have called list_files twice
        assert mock_ftp_client.list_files.call_count == 2


@pytest.mark.asyncio
async def test_daemon_stop(daemon_config, mock_ftp_client):
    """Test stopping the daemon."""
    with patch("radarlib.io.ftp.daemon.AsyncFTPClient", return_value=mock_ftp_client):
        daemon = FTPDaemon(daemon_config)

        # Start daemon in a task
        async def run_and_stop():
            task = asyncio.create_task(daemon.run())
            await asyncio.sleep(0.1)  # Let it start
            daemon.stop()
            await task

        await asyncio.wait_for(run_and_stop(), timeout=5.0)

        assert daemon._running is False


@pytest.mark.asyncio
async def test_daemon_get_processed_files(daemon_config, mock_ftp_client):
    """Test getting list of processed files."""
    local_dir = Path(daemon_config.local_dir)
    remote_files = ["/radar/data/file3.BUFR", "/radar/data/file1.BUFR", "/radar/data/file2.BUFR"]

    mock_ftp_client.list_files = AsyncMock(return_value=remote_files)
    mock_ftp_client.download_files = AsyncMock(
        return_value=[str(local_dir / "file3.BUFR"), str(local_dir / "file1.BUFR"), str(local_dir / "file2.BUFR")]
    )

    with patch("radarlib.io.ftp.daemon.AsyncFTPClient", return_value=mock_ftp_client):
        daemon = FTPDaemon(daemon_config)
        await daemon.run_once()

        processed = daemon.get_processed_files()

        # Should be sorted
        assert processed == ["file1.BUFR", "file2.BUFR", "file3.BUFR"]


@pytest.mark.asyncio
async def test_daemon_handles_download_errors(daemon_config, mock_ftp_client):
    """Test that daemon handles download errors gracefully."""
    mock_ftp_client.list_files = AsyncMock(side_effect=Exception("Connection error"))

    with patch("radarlib.io.ftp.daemon.AsyncFTPClient", return_value=mock_ftp_client):
        daemon = FTPDaemon(daemon_config)

        # Should not raise exception, should return 0
        count = await daemon.run_once()
        assert count == 0


@pytest.mark.asyncio
async def test_daemon_callback_error_handling(daemon_config, mock_ftp_client):
    """Test that daemon handles callback errors gracefully."""

    def failing_callback(local_path: str):
        raise Exception("Callback error")

    local_dir = Path(daemon_config.local_dir)
    remote_files = ["/radar/data/file1.BUFR"]
    downloaded_files = [str(local_dir / "file1.BUFR")]

    mock_ftp_client.list_files = AsyncMock(return_value=remote_files)
    mock_ftp_client.download_files = AsyncMock(return_value=downloaded_files)

    with patch("radarlib.io.ftp.daemon.AsyncFTPClient", return_value=mock_ftp_client):
        daemon = FTPDaemon(daemon_config, on_file_downloaded=failing_callback)

        # Should not raise exception even though callback fails
        count = await daemon.run_once()
        assert count == 1  # File was still downloaded


def test_daemon_config_creation():
    """Test creating daemon configuration."""
    config = FTPDaemonConfig(
        host="ftp.test.com",
        username="user",
        password="pass",
        remote_path="/data",
        local_dir="/local",
        port=2121,
        file_pattern="*.txt",
        poll_interval=30,
        max_concurrent_downloads=10,
        recursive=True,
    )

    assert config.host == "ftp.test.com"
    assert config.username == "user"
    assert config.password == "pass"
    assert config.remote_path == "/data"
    assert config.local_dir == "/local"
    assert config.port == 2121
    assert config.file_pattern == "*.txt"
    assert config.poll_interval == 30
    assert config.max_concurrent_downloads == 10
    assert config.recursive is True


def test_daemon_config_defaults():
    """Test daemon configuration default values."""
    config = FTPDaemonConfig(
        host="ftp.test.com", username="user", password="pass", remote_path="/data", local_dir="/local"
    )

    assert config.port == 21
    assert config.file_pattern == "*.BUFR"
    assert config.poll_interval == 60
    assert config.max_concurrent_downloads == 5
    assert config.recursive is False
