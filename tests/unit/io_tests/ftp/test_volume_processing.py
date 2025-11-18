# -*- coding: utf-8 -*-
"""Unit tests for volume processing functionality in SQLiteStateTracker."""

import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from radarlib.io.ftp.sqlite_state_tracker import SQLiteStateTracker


class TestVolumeProcessing:
    """Tests for volume processing methods in SQLiteStateTracker."""

    @pytest.fixture
    def tracker(self):
        """Create a temporary SQLiteStateTracker for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test_state.db"
            tracker = SQLiteStateTracker(db_path)
            yield tracker
            tracker.close()

    def test_get_volume_id(self, tracker):
        """Test volume ID generation."""
        volume_id = tracker.get_volume_id("RMA1", "0315", "01", "2025-11-18T12:30:00Z")
        assert volume_id == "RMA1_0315_01_2025-11-18T12:30:00Z"

    def test_register_volume_incomplete(self, tracker):
        """Test registering an incomplete volume."""
        volume_id = "RMA1_0315_01_2025-11-18T12:30:00Z"
        expected_fields = ["DBZH", "DBZV", "ZDR"]

        tracker.register_volume(
            volume_id,
            "RMA1",
            "0315",
            "01",
            "2025-11-18T12:30:00Z",
            expected_fields,
            is_complete=False,
        )

        # Check volume was registered
        volume_info = tracker.get_volume_info(volume_id)
        assert volume_info is not None
        assert volume_info["volume_id"] == volume_id
        assert volume_info["radar_code"] == "RMA1"
        assert volume_info["vol_code"] == "0315"
        assert volume_info["vol_number"] == "01"
        assert volume_info["status"] == "pending"
        assert volume_info["is_complete"] == 0
        assert volume_info["expected_fields"] == "DBZH,DBZV,ZDR"

    def test_register_volume_complete(self, tracker):
        """Test registering a complete volume."""
        volume_id = "RMA1_0315_01_2025-11-18T12:30:00Z"
        expected_fields = ["DBZH", "DBZV"]

        tracker.register_volume(
            volume_id,
            "RMA1",
            "0315",
            "01",
            "2025-11-18T12:30:00Z",
            expected_fields,
            is_complete=True,
        )

        volume_info = tracker.get_volume_info(volume_id)
        assert volume_info["is_complete"] == 1

    def test_update_volume_fields(self, tracker):
        """Test updating volume fields and completion status."""
        volume_id = "RMA1_0315_01_2025-11-18T12:30:00Z"
        tracker.register_volume(
            volume_id,
            "RMA1",
            "0315",
            "01",
            "2025-11-18T12:30:00Z",
            ["DBZH", "DBZV", "ZDR"],
            is_complete=False,
        )

        # Update with partial fields
        tracker.update_volume_fields(volume_id, ["DBZH"], False)
        volume_info = tracker.get_volume_info(volume_id)
        assert volume_info["downloaded_fields"] == "DBZH"
        assert volume_info["is_complete"] == 0

        # Update with all fields
        tracker.update_volume_fields(volume_id, ["DBZH", "DBZV", "ZDR"], True)
        volume_info = tracker.get_volume_info(volume_id)
        assert volume_info["downloaded_fields"] == "DBZH,DBZV,ZDR"
        assert volume_info["is_complete"] == 1

    def test_mark_volume_processing_status(self, tracker):
        """Test marking volume processing status."""
        volume_id = "RMA1_0315_01_2025-11-18T12:30:00Z"
        tracker.register_volume(
            volume_id,
            "RMA1",
            "0315",
            "01",
            "2025-11-18T12:30:00Z",
            ["DBZH"],
            is_complete=True,
        )

        # Mark as processing
        tracker.mark_volume_processing(volume_id, "processing")
        volume_info = tracker.get_volume_info(volume_id)
        assert volume_info["status"] == "processing"
        assert volume_info["processed_at"] is None

        # Mark as completed
        netcdf_path = "/path/to/output.nc"
        tracker.mark_volume_processing(volume_id, "completed", netcdf_path=netcdf_path)
        volume_info = tracker.get_volume_info(volume_id)
        assert volume_info["status"] == "completed"
        assert volume_info["netcdf_path"] == netcdf_path
        assert volume_info["processed_at"] is not None

    def test_mark_volume_failed(self, tracker):
        """Test marking volume as failed with error message."""
        volume_id = "RMA1_0315_01_2025-11-18T12:30:00Z"
        tracker.register_volume(
            volume_id,
            "RMA1",
            "0315",
            "01",
            "2025-11-18T12:30:00Z",
            ["DBZH"],
            is_complete=True,
        )

        error_msg = "Decoding failed: file corrupted"
        tracker.mark_volume_processing(volume_id, "failed", error_message=error_msg)

        volume_info = tracker.get_volume_info(volume_id)
        assert volume_info["status"] == "failed"
        assert volume_info["error_message"] == error_msg
        assert volume_info["netcdf_path"] is None

    def test_get_volumes_by_status(self, tracker):
        """Test getting volumes by status."""
        # Register multiple volumes with different statuses
        volumes = [
            ("RMA1_0315_01_2025-11-18T12:00:00Z", "pending"),
            ("RMA1_0315_01_2025-11-18T12:30:00Z", "pending"),
            ("RMA1_0315_02_2025-11-18T12:00:00Z", "processing"),
        ]

        for vol_id, status in volumes:
            parts = vol_id.split("_")
            tracker.register_volume(vol_id, parts[0], parts[1], parts[2], parts[3], ["DBZH"], is_complete=True)
            if status != "pending":
                tracker.mark_volume_processing(vol_id, status)

        # Get pending volumes
        pending = tracker.get_volumes_by_status("pending")
        assert len(pending) == 2

        # Get processing volumes
        processing = tracker.get_volumes_by_status("processing")
        assert len(processing) == 1

    def test_get_complete_unprocessed_volumes(self, tracker):
        """Test getting complete volumes that haven't been processed."""
        # Register complete and incomplete volumes
        complete_vol = "RMA1_0315_01_2025-11-18T12:00:00Z"
        incomplete_vol = "RMA1_0315_01_2025-11-18T12:30:00Z"

        tracker.register_volume(complete_vol, "RMA1", "0315", "01", "2025-11-18T12:00:00Z", ["DBZH"], is_complete=True)

        tracker.register_volume(
            incomplete_vol, "RMA1", "0315", "01", "2025-11-18T12:30:00Z", ["DBZH"], is_complete=False
        )

        # Get complete unprocessed volumes
        volumes = tracker.get_complete_unprocessed_volumes()
        assert len(volumes) == 1
        assert volumes[0]["volume_id"] == complete_vol
        assert volumes[0]["is_complete"] == 1
        assert volumes[0]["status"] == "pending"

        # Mark as processing and check again
        tracker.mark_volume_processing(complete_vol, "processing")
        volumes = tracker.get_complete_unprocessed_volumes()
        assert len(volumes) == 0

    def test_get_volume_files(self, tracker):
        """Test getting files belonging to a volume."""
        # Add some downloaded files
        files = [
            ("RMA1_0315_01_DBZH_20251118T123000Z.BUFR", "RMA1", "DBZH", "2025-11-18T12:30:00Z"),
            ("RMA1_0315_01_DBZV_20251118T123000Z.BUFR", "RMA1", "DBZV", "2025-11-18T12:30:00Z"),
            ("RMA1_0315_02_VRAD_20251118T123000Z.BUFR", "RMA1", "VRAD", "2025-11-18T12:30:00Z"),
        ]

        for filename, radar, field, obs_dt in files:
            tracker.mark_downloaded(
                filename,
                f"/L2/{radar}/2025/11/18/12/3000/{filename}",
                f"/local/{filename}",
                1000,
                "abc123",
                {"radar_code": radar, "field_type": field, "observation_datetime": obs_dt},
            )

        # Get files for volume 0315/01
        volume_files = tracker.get_volume_files("RMA1", "0315", "01", "2025-11-18T12:30:00Z")
        assert len(volume_files) == 2
        assert "RMA1_0315_01_DBZH_20251118T123000Z.BUFR" in volume_files
        assert "RMA1_0315_01_DBZV_20251118T123000Z.BUFR" in volume_files

        # Get files for volume 0315/02
        volume_files = tracker.get_volume_files("RMA1", "0315", "02", "2025-11-18T12:30:00Z")
        assert len(volume_files) == 1
        assert "RMA1_0315_02_VRAD_20251118T123000Z.BUFR" in volume_files

    def test_volume_lifecycle(self, tracker):
        """Test complete volume lifecycle from registration to completion."""
        volume_id = "RMA1_0315_01_2025-11-18T12:30:00Z"
        expected_fields = ["DBZH", "DBZV"]

        # Step 1: Register incomplete volume
        tracker.register_volume(
            volume_id,
            "RMA1",
            "0315",
            "01",
            "2025-11-18T12:30:00Z",
            expected_fields,
            is_complete=False,
        )
        assert len(tracker.get_complete_unprocessed_volumes()) == 0

        # Step 2: Update with one field
        tracker.update_volume_fields(volume_id, ["DBZH"], False)
        volume_info = tracker.get_volume_info(volume_id)
        assert volume_info["is_complete"] == 0

        # Step 3: Complete the volume
        tracker.update_volume_fields(volume_id, ["DBZH", "DBZV"], True)
        volume_info = tracker.get_volume_info(volume_id)
        assert volume_info["is_complete"] == 1
        assert len(tracker.get_complete_unprocessed_volumes()) == 1

        # Step 4: Start processing
        tracker.mark_volume_processing(volume_id, "processing")
        assert len(tracker.get_complete_unprocessed_volumes()) == 0
        assert len(tracker.get_volumes_by_status("processing")) == 1

        # Step 5: Complete processing
        tracker.mark_volume_processing(volume_id, "completed", netcdf_path="/output/file.nc")
        volume_info = tracker.get_volume_info(volume_id)
        assert volume_info["status"] == "completed"
        assert volume_info["netcdf_path"] == "/output/file.nc"
        assert volume_info["processed_at"] is not None
        assert len(tracker.get_volumes_by_status("completed")) == 1
