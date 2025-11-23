"""Tests for SQLiteStateTracker."""

from datetime import datetime, timedelta, timezone

from radarlib.io.ftp import SQLiteStateTracker


class TestSQLiteStateTracker:
    """Test suite for SQLiteStateTracker class."""

    def test_init_new_database(self, tmp_path):
        """Test initialization with new database."""
        db_file = tmp_path / "state.db"
        tracker = SQLiteStateTracker(db_file)

        assert tracker.count() == 0
        assert tracker.db_path == db_file
        assert db_file.exists()

        tracker.close()

    def test_mark_downloaded(self, tmp_path):
        """Test marking a file as downloaded."""
        db_file = tmp_path / "state.db"
        tracker = SQLiteStateTracker(db_file)

        tracker.mark_downloaded("file1.BUFR", "/L2/RMA1/2025/01/01/00/0000/file1.BUFR")

        assert tracker.is_downloaded("file1.BUFR")
        assert tracker.count() == 1

        tracker.close()

    # def test_mark_downloaded_with_metadata(self, tmp_path):
    #     """Test marking file with metadata."""
    #     db_file = tmp_path / "state.db"
    #     tracker = SQLiteStateTracker(db_file)

    #     metadata = {
    #         "radar_code": "RMA1",
    #         "field_type": "DBZH",
    #         "observation_datetime": "2025-01-01T00:00:00+00:00",
    #     }

    #     tracker.mark_downloaded(
    #         "file1.BUFR",
    #         "/L2/RMA1/2025/01/01/00/0000/file1.BUFR",
    #         "/local/file1.BUFR",
    #         1024,
    #         "abc123",
    #         metadata,
    #     )

    #     info = tracker.get_file_info("file1.BUFR")
    #     assert info is not None
    #     assert info["radar_code"] == "RMA1"
    #     assert info["field_type"] == "DBZH"
    #     assert info["file_size"] == 1024
    #     assert info["checksum"] == "abc123"

    #     tracker.close()

    def test_is_downloaded_false(self, tmp_path):
        """Test checking if non-downloaded file returns False."""
        db_file = tmp_path / "state.db"
        tracker = SQLiteStateTracker(db_file)

        assert tracker.is_downloaded("nonexistent.BUFR") is False

        tracker.close()

    def test_get_downloaded_files(self, tmp_path):
        """Test getting set of downloaded files."""
        db_file = tmp_path / "state.db"
        tracker = SQLiteStateTracker(db_file)

        tracker.mark_downloaded("file1.BUFR", "/L2/file1.BUFR")
        tracker.mark_downloaded("file2.BUFR", "/L2/file2.BUFR")

        downloaded = tracker.get_downloaded_files()

        assert len(downloaded) == 2
        assert "file1.BUFR" in downloaded
        assert "file2.BUFR" in downloaded

        tracker.close()

    def test_get_file_info_not_found(self, tmp_path):
        """Test getting info for non-existent file."""
        db_file = tmp_path / "state.db"
        tracker = SQLiteStateTracker(db_file)

        info = tracker.get_file_info("nonexistent.BUFR")
        assert info is None

        tracker.close()

    def test_clear(self, tmp_path):
        """Test clearing all state."""
        db_file = tmp_path / "state.db"
        tracker = SQLiteStateTracker(db_file)

        tracker.mark_downloaded("file1.BUFR", "/L2/file1.BUFR")
        tracker.mark_downloaded("file2.BUFR", "/L2/file2.BUFR")

        assert tracker.count() == 2

        tracker.clear()

        assert tracker.count() == 0
        assert not tracker.is_downloaded("file1.BUFR")

        tracker.close()

    def test_remove_file(self, tmp_path):
        """Test removing a file from state."""
        db_file = tmp_path / "state.db"
        tracker = SQLiteStateTracker(db_file)

        tracker.mark_downloaded("file1.BUFR", "/L2/file1.BUFR")
        tracker.mark_downloaded("file2.BUFR", "/L2/file2.BUFR")

        tracker.remove_file("file1.BUFR")

        assert not tracker.is_downloaded("file1.BUFR")
        assert tracker.is_downloaded("file2.BUFR")
        assert tracker.count() == 1

        tracker.close()

    def test_persistence(self, tmp_path):
        """Test that state persists across tracker instances."""
        db_file = tmp_path / "state.db"

        # First tracker
        tracker1 = SQLiteStateTracker(db_file)
        tracker1.mark_downloaded("file1.BUFR", "/L2/file1.BUFR")
        tracker1.close()

        # Second tracker
        tracker2 = SQLiteStateTracker(db_file)
        assert tracker2.is_downloaded("file1.BUFR")
        assert tracker2.count() == 1
        tracker2.close()

    def test_get_files_by_date_range(self, tmp_path):
        """Test filtering files by date range."""
        db_file = tmp_path / "state.db"
        tracker = SQLiteStateTracker(db_file)

        now = datetime.now(timezone.utc)
        yesterday = now - timedelta(days=1)
        week_ago = now - timedelta(days=7)

        # Add files with different dates
        tracker.mark_downloaded(
            "file1.BUFR",
            "/L2/file1.BUFR",
            observation_datetime=now.isoformat(),
            radar_name="RMA1",
        )
        tracker.mark_downloaded(
            "file2.BUFR",
            "/L2/file2.BUFR",
            observation_datetime=yesterday.isoformat(),
            radar_name="RMA1",
        )
        tracker.mark_downloaded(
            "file3.BUFR",
            "/L2/file3.BUFR",
            observation_datetime=week_ago.isoformat(),
            radar_name="RMA1",
        )

        # Get files from last 2 days
        start = now - timedelta(days=2)
        end = now + timedelta(hours=1)

        files = tracker.get_files_by_date_range(start, end, radar_name="RMA1")

        assert len(files) == 2
        assert "file1.BUFR" in files
        assert "file2.BUFR" in files
        assert "file3.BUFR" not in files

        tracker.close()

    def test_calculate_checksum(self, tmp_path):
        """Test checksum calculation."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("Hello, World!")

        checksum = SQLiteStateTracker.calculate_checksum(test_file)

        # SHA256 of "Hello, World!"
        expected = "dffd6021bb2bd5b0af676290809ec3a53191dd81c7f70a4b28688a362182986f"
        assert checksum == expected
