"""Tests for FTP client."""

import ftplib
from pathlib import Path
from unittest.mock import MagicMock, Mock, mock_open, patch

import pytest

from radarlib.io.ftp import FTPActionError, FTPClient, FTP_IsADirectoryError


class TestFTPClient:
    """Test suite for FTPClient class."""

    def test_init(self):
        """Test FTPClient initialization."""
        client = FTPClient(host="ftp.example.com", user="user", password="pass")
        assert client.host == "ftp.example.com"
        assert client.user == "user"
        assert client.password == "pass"

    @patch("radarlib.io.ftp.client.ftp_connection_manager")
    def test_list_files_nlst(self, mock_conn_mgr):
        """Test listing files using nlst method."""
        # Setup mock
        mock_ftp = MagicMock()
        mock_ftp.nlst.return_value = ["file1.BUFR", "file2.BUFR"]
        mock_conn_mgr.return_value.__enter__.return_value = mock_ftp

        client = FTPClient(host="ftp.example.com", user="user", password="pass")
        files = client.list_files("/L2/RMA1")

        # Verify
        assert files == ["file1.BUFR", "file2.BUFR"]
        mock_ftp.cwd.assert_called_once_with("/L2/RMA1")
        mock_ftp.nlst.assert_called_once()

    @patch("radarlib.io.ftp.client.ftp_connection_manager")
    def test_list_files_mlsd(self, mock_conn_mgr):
        """Test listing files using mlsd method."""
        # Setup mock
        mock_ftp = MagicMock()
        mock_ftp.mlsd.return_value = [("file1.BUFR", {}), ("file2.BUFR", {})]
        mock_conn_mgr.return_value.__enter__.return_value = mock_ftp

        client = FTPClient(host="ftp.example.com", user="user", password="pass")
        files = client.list_files("/L2/RMA1", method="mlsd")

        # Verify
        assert len(files) == 2
        mock_ftp.cwd.assert_called_once_with("/L2/RMA1")
        mock_ftp.mlsd.assert_called_once()

    @patch("radarlib.io.ftp.client.ftp_connection_manager")
    def test_list_files_error(self, mock_conn_mgr):
        """Test error handling when listing files."""
        # Setup mock to raise error
        mock_ftp = MagicMock()
        mock_ftp.cwd.side_effect = ftplib.error_perm("550 Directory not found")
        mock_conn_mgr.return_value.__enter__.return_value = mock_ftp

        client = FTPClient(host="ftp.example.com", user="user", password="pass")

        with pytest.raises(FTPActionError) as exc_info:
            client.list_files("/nonexistent")

        assert "Failed to list directory" in str(exc_info.value)

    @patch("radarlib.io.ftp.client.ftp_connection_manager")
    @patch("builtins.open", new_callable=mock_open)
    def test_download_file_success(self, mock_file, mock_conn_mgr):
        """Test successful file download."""
        # Setup mock
        mock_ftp = MagicMock()
        # Mock cwd to fail for file verification (it's not a directory)
        mock_ftp.cwd.side_effect = [
            None,  # Initial cwd to remote_dir
            ftplib.error_perm("Not a directory"),  # file verification fails (good)
        ]
        mock_conn_mgr.return_value.__enter__.return_value = mock_ftp

        client = FTPClient(host="ftp.example.com", user="user", password="pass")
        client.download_file("/L2/RMA1/file.BUFR", Path("/local/file.BUFR"))

        # Verify
        assert mock_ftp.cwd.call_count == 2
        mock_ftp.retrbinary.assert_called_once()
        assert "RETR file.BUFR" in mock_ftp.retrbinary.call_args[0][0]

    @patch("radarlib.io.ftp.client.ftp_connection_manager")
    def test_download_file_is_directory(self, mock_conn_mgr):
        """Test that downloading a directory raises error."""
        # Setup mock - cwd succeeds meaning it's a directory
        mock_ftp = MagicMock()
        mock_ftp.cwd.side_effect = [
            None,  # Initial cwd to remote_dir
            None,  # cwd to "file" succeeds (it's a directory!)
            None,  # cwd back to parent
        ]
        mock_conn_mgr.return_value.__enter__.return_value = mock_ftp

        client = FTPClient(host="ftp.example.com", user="user", password="pass")

        with pytest.raises(FTP_IsADirectoryError) as exc_info:
            client.download_file("/L2/RMA1/somedir", Path("/local/somedir"))

        assert "is a directory" in str(exc_info.value)

    @patch("radarlib.io.ftp.client.ftp_connection_manager")
    @patch("builtins.open", new_callable=mock_open)
    def test_download_files_success(self, mock_file, mock_conn_mgr):
        """Test downloading multiple files."""
        # Setup mock
        mock_ftp = MagicMock()
        # Mock cwd to fail for files (they're not directories)
        mock_ftp.cwd.side_effect = [
            None,  # Initial cwd to remote_dir
            ftplib.error_perm("Not a directory"),  # file1 verification
            ftplib.error_perm("Not a directory"),  # file2 verification
        ]
        mock_conn_mgr.return_value.__enter__.return_value = mock_ftp

        client = FTPClient(host="ftp.example.com", user="user", password="pass")
        client.download_files(
            "/L2/RMA1",
            ["file1.BUFR", "file2.BUFR"],
            Path("/local")
        )

        # Verify retrbinary called for each file
        assert mock_ftp.retrbinary.call_count == 2

    @patch("radarlib.io.ftp.client.ftp_connection_manager")
    def test_file_exists_true(self, mock_conn_mgr):
        """Test checking if file exists (returns True)."""
        # Setup mock
        mock_ftp = MagicMock()
        mock_ftp.nlst.return_value = ["file1.BUFR", "file2.BUFR"]
        mock_conn_mgr.return_value.__enter__.return_value = mock_ftp

        client = FTPClient(host="ftp.example.com", user="user", password="pass")
        exists = client.file_exists("/L2/RMA1/file1.BUFR")

        assert exists is True

    @patch("radarlib.io.ftp.client.ftp_connection_manager")
    def test_file_exists_false(self, mock_conn_mgr):
        """Test checking if file exists (returns False)."""
        # Setup mock
        mock_ftp = MagicMock()
        mock_ftp.nlst.return_value = ["file1.BUFR", "file2.BUFR"]
        mock_conn_mgr.return_value.__enter__.return_value = mock_ftp

        client = FTPClient(host="ftp.example.com", user="user", password="pass")
        exists = client.file_exists("/L2/RMA1/nonexistent.BUFR")

        assert exists is False

    @patch("radarlib.io.ftp.client.ftp_connection_manager")
    def test_file_exists_connection_error(self, mock_conn_mgr):
        """Test file_exists returns False on connection error."""
        # Setup mock to raise error
        mock_conn_mgr.side_effect = ConnectionError("Connection failed")

        client = FTPClient(host="ftp.example.com", user="user", password="pass")
        exists = client.file_exists("/L2/RMA1/file.BUFR")

        assert exists is False
