"""Tests for FileStateTracker."""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from radarlib.io.ftp import FileStateTracker


class TestFileStateTracker:
    """Test suite for FileStateTracker class."""

    def test_init_new_state_file(self, tmp_path):
        """Test initialization with new state file."""
        state_file = tmp_path / "state.json"
        tracker = FileStateTracker(state_file)

        assert tracker.count() == 0
        assert tracker.state_file == state_file

    def test_init_existing_state_file(self, tmp_path):
        """Test initialization with existing state file."""
        state_file = tmp_path / "state.json"

        # Create existing state
        existing_state = {
            "file1.BUFR": {
                "remote_path": "/L2/file1.BUFR",
                "downloaded_at": "2024-01-01T12:00:00",
                "metadata": {}
            }
        }
        with open(state_file, "w") as f:
            json.dump(existing_state, f)

        # Load tracker
        tracker = FileStateTracker(state_file)

        assert tracker.count() == 1
        assert tracker.is_downloaded("file1.BUFR")

    def test_mark_downloaded(self, tmp_path):
        """Test marking a file as downloaded."""
        state_file = tmp_path / "state.json"
        tracker = FileStateTracker(state_file)

        tracker.mark_downloaded("file1.BUFR", "/L2/file1.BUFR")

        assert tracker.is_downloaded("file1.BUFR")
        assert tracker.count() == 1

        # Verify state was saved
        with open(state_file, "r") as f:
            saved_state = json.load(f)

        assert "file1.BUFR" in saved_state

    def test_mark_downloaded_with_metadata(self, tmp_path):
        """Test marking file with metadata."""
        state_file = tmp_path / "state.json"
        tracker = FileStateTracker(state_file)

        metadata = {
            "radar": "RMA1",
            "field": "DBZH",
            "timestamp": "20240101T120000Z"
        }

        tracker.mark_downloaded("file1.BUFR", "/L2/file1.BUFR", metadata)

        info = tracker.get_file_info("file1.BUFR")
        assert info is not None
        assert info["metadata"] == metadata

    def test_is_downloaded_false(self, tmp_path):
        """Test checking if non-downloaded file returns False."""
        state_file = tmp_path / "state.json"
        tracker = FileStateTracker(state_file)

        assert tracker.is_downloaded("nonexistent.BUFR") is False

    def test_get_downloaded_files(self, tmp_path):
        """Test getting set of downloaded files."""
        state_file = tmp_path / "state.json"
        tracker = FileStateTracker(state_file)

        tracker.mark_downloaded("file1.BUFR", "/L2/file1.BUFR")
        tracker.mark_downloaded("file2.BUFR", "/L2/file2.BUFR")

        downloaded = tracker.get_downloaded_files()

        assert len(downloaded) == 2
        assert "file1.BUFR" in downloaded
        assert "file2.BUFR" in downloaded

    def test_get_file_info_not_found(self, tmp_path):
        """Test getting info for non-existent file."""
        state_file = tmp_path / "state.json"
        tracker = FileStateTracker(state_file)

        info = tracker.get_file_info("nonexistent.BUFR")
        assert info is None

    def test_clear(self, tmp_path):
        """Test clearing all state."""
        state_file = tmp_path / "state.json"
        tracker = FileStateTracker(state_file)

        tracker.mark_downloaded("file1.BUFR", "/L2/file1.BUFR")
        tracker.mark_downloaded("file2.BUFR", "/L2/file2.BUFR")

        assert tracker.count() == 2

        tracker.clear()

        assert tracker.count() == 0
        assert not tracker.is_downloaded("file1.BUFR")

    def test_remove_file(self, tmp_path):
        """Test removing a file from state."""
        state_file = tmp_path / "state.json"
        tracker = FileStateTracker(state_file)

        tracker.mark_downloaded("file1.BUFR", "/L2/file1.BUFR")
        tracker.mark_downloaded("file2.BUFR", "/L2/file2.BUFR")

        tracker.remove_file("file1.BUFR")

        assert not tracker.is_downloaded("file1.BUFR")
        assert tracker.is_downloaded("file2.BUFR")
        assert tracker.count() == 1

    def test_remove_file_not_exists(self, tmp_path):
        """Test removing non-existent file doesn't error."""
        state_file = tmp_path / "state.json"
        tracker = FileStateTracker(state_file)

        # Should not raise error
        tracker.remove_file("nonexistent.BUFR")

    def test_persistence(self, tmp_path):
        """Test that state persists across tracker instances."""
        state_file = tmp_path / "state.json"

        # First tracker
        tracker1 = FileStateTracker(state_file)
        tracker1.mark_downloaded("file1.BUFR", "/L2/file1.BUFR")

        # Second tracker
        tracker2 = FileStateTracker(state_file)

        assert tracker2.is_downloaded("file1.BUFR")
        assert tracker2.count() == 1

    def test_get_files_by_date_range(self, tmp_path):
        """Test filtering files by date range."""
        state_file = tmp_path / "state.json"
        tracker = FileStateTracker(state_file)

        # Manually create state with specific dates
        now = datetime.now(timezone.utc)
        yesterday = now - timedelta(days=1)
        week_ago = now - timedelta(days=7)

        tracker._state = {
            "file1.BUFR": {
                "remote_path": "/L2/file1.BUFR",
                "downloaded_at": now.isoformat(),
                "metadata": {}
            },
            "file2.BUFR": {
                "remote_path": "/L2/file2.BUFR",
                "downloaded_at": yesterday.isoformat(),
                "metadata": {}
            },
            "file3.BUFR": {
                "remote_path": "/L2/file3.BUFR",
                "downloaded_at": week_ago.isoformat(),
                "metadata": {}
            }
        }

        # Get files from last 2 days
        start = now - timedelta(days=2)
        end = now + timedelta(hours=1)

        files = tracker.get_files_by_date_range(start, end)

        assert len(files) == 2
        assert "file1.BUFR" in files
        assert "file2.BUFR" in files
        assert "file3.BUFR" not in files

    def test_corrupted_state_file(self, tmp_path):
        """Test that corrupted state file doesn't crash."""
        state_file = tmp_path / "state.json"

        # Create corrupted JSON
        with open(state_file, "w") as f:
            f.write("{invalid json")

        # Should not raise, just start with empty state
        tracker = FileStateTracker(state_file)

        assert tracker.count() == 0
