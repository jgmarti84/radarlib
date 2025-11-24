"""Integration tests for FTP client with mocked FTP server."""

import ftplib
from unittest.mock import MagicMock, patch

import pytest

from radarlib.io.ftp import FTPClient
from radarlib.state import FileStateTracker


@pytest.mark.integration
class TestFTPClientIntegration:
    """Integration tests for FTPClient with mocked server."""

    @patch("radarlib.io.ftp.ftp.ftplib.FTP")
    def test_full_download_workflow(self, mock_ftp_class, tmp_path):
        """Test complete workflow: list, filter, download, track."""
        # Setup mock FTP server
        mock_ftp = MagicMock()
        mock_ftp.nlst.return_value = [
            "RMA1_0315_01_DBZH_20240101T120000Z.BUFR",
            "RMA1_0315_01_VRAD_20240101T120000Z.BUFR",
            "readme.txt",  # Non-BUFR file
        ]

        def mock_retrbinary(cmd, callback):
            # Simulate file download
            callback(b"mock BUFR data")

        mock_ftp.retrbinary = mock_retrbinary

        # Mock cwd - always succeed for directories, fail for file checks
        def mock_cwd(path):
            # If path looks like a file, fail
            if path.endswith(".BUFR"):
                raise ftplib.error_perm("Not a directory")
            return None

        mock_ftp.cwd = mock_cwd
        mock_ftp_class.return_value = mock_ftp

        # Setup client and tracker
        client = FTPClient(host="test.ftp.com", user="test", password="test")
        tracker = FileStateTracker(tmp_path / "state.json")
        local_dir = tmp_path / "downloads"
        local_dir.mkdir()

        # 1. List files
        files = client.list_files("/L2/RMA1")
        assert len(files) == 3

        # 2. Filter BUFR files
        bufr_files = [f for f in files if f.endswith(".BUFR")]
        assert len(bufr_files) == 2

        # 3. Filter already downloaded (none yet)
        new_files = [f for f in bufr_files if not tracker.is_downloaded(f)]
        assert len(new_files) == 2

        # 4. Download files
        for filename in new_files:
            remote_path = f"/L2/RMA1/{filename}"
            local_path = local_dir / filename

            client.download_file(remote_path, local_path)
            tracker.mark_downloaded(filename, remote_path)

            # Verify file exists and has content
            assert local_path.exists()
            assert local_path.stat().st_size > 0

        # 5. Verify state
        assert tracker.count() == 2

        # 6. List again - should find no new files
        new_files = [f for f in bufr_files if not tracker.is_downloaded(f)]
        assert len(new_files) == 0

    @patch("radarlib.io.ftp.ftp.ftplib.FTP")
    def test_selective_download_by_field(self, mock_ftp_class, tmp_path):
        """Test downloading only specific field types."""
        # Setup mock
        mock_ftp = MagicMock()
        mock_ftp.nlst.return_value = [
            "RMA1",  # Directory
        ]

        # Simulate directory listing for RMA1
        def mock_cwd(path):
            if path.endswith("RMA1"):
                # Return files in this directory
                mock_ftp.nlst.return_value = [
                    "RMA1_0315_01_DBZH_20240101T120000Z.BUFR",
                    "RMA1_0315_01_VRAD_20240101T120000Z.BUFR",
                ]

        mock_ftp.cwd.side_effect = mock_cwd
        mock_ftp_class.return_value = mock_ftp

        # Setup client
        client = FTPClient(host="test.ftp.com", user="test", password="test")
        local_dir = tmp_path / "downloads"
        local_dir.mkdir()

        # List and filter for DBZH only
        files = client.list_files("/L2/RMA1")

        dbzh_files = []
        for filename in files:
            if filename.endswith(".BUFR"):
                # Parse to get field type
                if "DBZH" in filename:
                    dbzh_files.append(filename)

        assert len(dbzh_files) == 1
        assert "DBZH" in dbzh_files[0]

    @patch("radarlib.io.ftp.ftp.ftplib.FTP")
    def test_state_persistence_across_sessions(self, mock_ftp_class, tmp_path):
        """Test that state persists across multiple client sessions."""
        # Setup mock
        mock_ftp = MagicMock()
        mock_ftp.nlst.return_value = ["file1.BUFR", "file2.BUFR"]
        mock_ftp_class.return_value = mock_ftp

        state_file = tmp_path / "state.json"

        # Session 1: Download file1
        tracker1 = FileStateTracker(state_file)
        tracker1.mark_downloaded("file1.BUFR", "/L2/file1.BUFR")
        assert tracker1.count() == 1

        # Session 2: Load existing state and add file2
        tracker2 = FileStateTracker(state_file)
        assert tracker2.count() == 1  # Should load existing state
        assert tracker2.is_downloaded("file1.BUFR")

        tracker2.mark_downloaded("file2.BUFR", "/L2/file2.BUFR")
        assert tracker2.count() == 2

        # Session 3: Verify both files tracked
        tracker3 = FileStateTracker(state_file)
        assert tracker3.count() == 2
        assert tracker3.is_downloaded("file1.BUFR")
        assert tracker3.is_downloaded("file2.BUFR")
