# -*- coding: utf-8 -*-
"""Tests for the state module."""

import pytest
from datetime import datetime, timedelta, timezone


class TestStateImport:
    """Test that state tracking classes can be imported from the new location."""

    def test_import_sqlite_tracker(self):
        """Test importing SQLiteStateTracker from new location."""
        from radarlib.state import SQLiteStateTracker
        assert SQLiteStateTracker is not None

    def test_import_file_tracker(self):
        """Test importing FileStateTracker from new location."""
        from radarlib.state import FileStateTracker
        assert FileStateTracker is not None


class TestSQLiteTrackerNewLocation:
    """Tests for SQLiteStateTracker using the new location."""

    def test_sqlite_tracker_init(self, tmp_path):
        """Test SQLiteStateTracker initialization from new location."""
        from radarlib.state import SQLiteStateTracker

        db_file = tmp_path / "state.db"
        tracker = SQLiteStateTracker(db_file)

        assert tracker.count() == 0
        assert tracker.db_path == db_file
        assert db_file.exists()

        tracker.close()

    def test_sqlite_tracker_mark_downloaded(self, tmp_path):
        """Test marking a file as downloaded using new location import."""
        from radarlib.state import SQLiteStateTracker

        db_file = tmp_path / "state.db"
        tracker = SQLiteStateTracker(db_file)

        tracker.mark_downloaded("file1.BUFR", "/L2/RMA1/file1.BUFR")

        assert tracker.is_downloaded("file1.BUFR")
        assert tracker.count() == 1

        tracker.close()


class TestFileTrackerNewLocation:
    """Tests for FileStateTracker using the new location."""

    def test_file_tracker_init(self, tmp_path):
        """Test FileStateTracker initialization from new location."""
        from radarlib.state import FileStateTracker

        state_file = tmp_path / "state.json"
        tracker = FileStateTracker(state_file)

        assert tracker.count() == 0

    def test_file_tracker_mark_downloaded(self, tmp_path):
        """Test marking a file as downloaded using new location import."""
        from radarlib.state import FileStateTracker

        state_file = tmp_path / "state.json"
        tracker = FileStateTracker(state_file)

        tracker.mark_downloaded("file1.BUFR", "/L2/file1.BUFR")

        assert tracker.is_downloaded("file1.BUFR")
        assert tracker.count() == 1
