"""
Unit tests for the async FTP client.

These tests use mocking to avoid requiring an actual FTP server.
"""

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from radarlib.io.ftp.client import AsyncFTPClient


@pytest.fixture
def mock_aioftp_client():
    """Create a mock aioftp.Client instance."""
    mock_client = AsyncMock()
    mock_client.connect = AsyncMock()
    mock_client.login = AsyncMock()
    mock_client.quit = AsyncMock()
    mock_client.list = AsyncMock()
    mock_client.download = AsyncMock()
    mock_client.stat = AsyncMock()
    return mock_client


@pytest.mark.asyncio
async def test_client_connect(mock_aioftp_client):
    """Test FTP client connection."""
    with patch("radarlib.io.ftp.client.aioftp.Client", return_value=mock_aioftp_client):
        client = AsyncFTPClient("ftp.example.com", "user", "pass")
        await client.connect()

        mock_aioftp_client.connect.assert_called_once_with("ftp.example.com", 21)
        mock_aioftp_client.login.assert_called_once_with("user", "pass")
        assert client._client is not None


@pytest.mark.asyncio
async def test_client_disconnect(mock_aioftp_client):
    """Test FTP client disconnection."""
    with patch("radarlib.io.ftp.client.aioftp.Client", return_value=mock_aioftp_client):
        client = AsyncFTPClient("ftp.example.com", "user", "pass")
        await client.connect()
        await client.disconnect()

        mock_aioftp_client.quit.assert_called_once()
        assert client._client is None


@pytest.mark.asyncio
async def test_client_context_manager(mock_aioftp_client):
    """Test FTP client context manager."""
    with patch("radarlib.io.ftp.client.aioftp.Client", return_value=mock_aioftp_client):
        async with AsyncFTPClient("ftp.example.com", "user", "pass") as client:
            assert client._client is not None

        mock_aioftp_client.quit.assert_called_once()


@pytest.mark.asyncio
async def test_list_files_bufr_pattern(mock_aioftp_client):
    """Test listing BUFR files with pattern matching."""

    # Mock file listing
    async def mock_list(path, recursive=False):
        files = [
            (Path("/radar/data/file1.BUFR"), {"type": "file"}),
            (Path("/radar/data/file2.BUFR"), {"type": "file"}),
            (Path("/radar/data/file3.txt"), {"type": "file"}),
            (Path("/radar/data/subdir"), {"type": "dir"}),
        ]
        for item in files:
            yield item

    mock_aioftp_client.list = mock_list

    with patch("radarlib.io.ftp.client.aioftp.Client", return_value=mock_aioftp_client):
        async with AsyncFTPClient("ftp.example.com", "user", "pass") as client:
            files = await client.list_files("/radar/data", pattern="*.BUFR")

            assert len(files) == 2
            assert "/radar/data/file1.BUFR" in files
            assert "/radar/data/file2.BUFR" in files
            assert "/radar/data/file3.txt" not in files


@pytest.mark.asyncio
async def test_list_files_not_connected():
    """Test that list_files raises error when not connected."""
    client = AsyncFTPClient("ftp.example.com", "user", "pass")

    with pytest.raises(ValueError, match="Not connected to FTP server"):
        await client.list_files("/radar/data")


@pytest.mark.asyncio
async def test_download_file(mock_aioftp_client, tmp_path):
    """Test downloading a single file."""
    with patch("radarlib.io.ftp.client.aioftp.Client", return_value=mock_aioftp_client):
        async with AsyncFTPClient("ftp.example.com", "user", "pass") as client:
            local_path = await client.download_file("/radar/data/test.BUFR", str(tmp_path))

            expected_path = tmp_path / "test.BUFR"
            assert local_path == str(expected_path)
            mock_aioftp_client.download.assert_called_once()


@pytest.mark.asyncio
async def test_download_file_not_connected():
    """Test that download_file raises error when not connected."""
    client = AsyncFTPClient("ftp.example.com", "user", "pass")

    with pytest.raises(ValueError, match="Not connected to FTP server"):
        await client.download_file("/radar/data/test.BUFR", "/tmp")


@pytest.mark.asyncio
async def test_download_multiple_files(mock_aioftp_client, tmp_path):
    """Test downloading multiple files concurrently."""
    with patch("radarlib.io.ftp.client.aioftp.Client", return_value=mock_aioftp_client):
        async with AsyncFTPClient("ftp.example.com", "user", "pass") as client:
            files = ["/radar/data/file1.BUFR", "/radar/data/file2.BUFR", "/radar/data/file3.BUFR"]

            downloaded = await client.download_files(files, str(tmp_path), max_concurrent=2)

            assert len(downloaded) == 3
            assert mock_aioftp_client.download.call_count == 3


@pytest.mark.asyncio
async def test_download_files_with_failures(mock_aioftp_client, tmp_path):
    """Test that download_files handles partial failures gracefully."""

    # Make some downloads fail
    call_count = 0

    async def mock_download(remote, local, write_into=True):
        nonlocal call_count
        call_count += 1
        if call_count == 2:  # Second call fails
            raise Exception("Download failed")

    mock_aioftp_client.download = mock_download

    with patch("radarlib.io.ftp.client.aioftp.Client", return_value=mock_aioftp_client):
        async with AsyncFTPClient("ftp.example.com", "user", "pass") as client:
            files = ["/radar/data/file1.BUFR", "/radar/data/file2.BUFR", "/radar/data/file3.BUFR"]

            downloaded = await client.download_files(files, str(tmp_path))

            # Should have 2 successful downloads (file2 failed)
            assert len(downloaded) == 2


@pytest.mark.asyncio
async def test_check_file_exists(mock_aioftp_client):
    """Test checking if a file exists."""
    with patch("radarlib.io.ftp.client.aioftp.Client", return_value=mock_aioftp_client):
        async with AsyncFTPClient("ftp.example.com", "user", "pass") as client:
            # File exists
            mock_aioftp_client.stat = AsyncMock()
            exists = await client.check_file_exists("/radar/data/test.BUFR")
            assert exists is True

            # File doesn't exist
            from aioftp.errors import StatusCodeError

            mock_aioftp_client.stat = AsyncMock(side_effect=StatusCodeError("550", "550", "File not found"))
            exists = await client.check_file_exists("/radar/data/missing.BUFR")
            assert exists is False


@pytest.mark.asyncio
async def test_check_file_exists_not_connected():
    """Test that check_file_exists raises error when not connected."""
    client = AsyncFTPClient("ftp.example.com", "user", "pass")

    with pytest.raises(ValueError, match="Not connected to FTP server"):
        await client.check_file_exists("/radar/data/test.BUFR")


def test_client_initialization():
    """Test FTP client initialization with default parameters."""
    client = AsyncFTPClient("ftp.example.com")

    assert client.host == "ftp.example.com"
    assert client.username == "anonymous"
    assert client.password == ""
    assert client.port == 21


def test_client_initialization_with_params():
    """Test FTP client initialization with custom parameters."""
    client = AsyncFTPClient("ftp.example.com", username="testuser", password="testpass", port=2121)

    assert client.host == "ftp.example.com"
    assert client.username == "testuser"
    assert client.password == "testpass"
    assert client.port == 2121
