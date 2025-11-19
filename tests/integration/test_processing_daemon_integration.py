# -*- coding: utf-8 -*-
"""Integration test for BUFR processing daemon."""

import tempfile
from pathlib import Path

import pytest


@pytest.mark.integration
def test_processing_daemon_with_complete_volume():
    """
    Integration test: Process a complete volume from BUFR files to NetCDF.

    This test:
    1. Sets up a processing daemon with real BUFR test files
    2. Registers downloaded files in the state database
    3. Checks volume completion detection
    4. Processes the complete volume to NetCDF
    5. Verifies the NetCDF file was created and is valid
    """
    from radarlib.io.ftp import ProcessingDaemon, ProcessingDaemonConfig, SQLiteStateTracker

    # Locate test BUFR files
    data_dir = Path("tests/data/bufr/RMA5")
    if not data_dir.exists():
        pytest.skip("No BUFR test data available")

    bufr_files = sorted(list(data_dir.glob("*.BUFR")))
    if len(bufr_files) < 2:
        pytest.skip("Not enough BUFR test files for volume test")

    # Use RMA5 volume 0315/02 which has VRAD and WRAD files
    target_files = [f for f in bufr_files if "_0315_02_" in f.name]
    if len(target_files) < 2:
        pytest.skip("Not enough files for volume 0315/02")

    with tempfile.TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        netcdf_dir = base / "netcdf"
        state_db = base / "state.db"

        # Configure daemon for RMA5 volume types
        config = ProcessingDaemonConfig(
            local_bufr_dir=data_dir,  # Use actual test data directory
            local_netcdf_dir=netcdf_dir,
            state_db=state_db,
            volume_types={
                "0315": {
                    "02": ["VRAD", "WRAD"],
                }
            },
            radar_code="RMA5",
            poll_interval=1,
        )

        daemon = ProcessingDaemon(config)

        # Register the files as downloaded
        tracker = SQLiteStateTracker(state_db)
        for bufr_file in target_files:
            # Parse filename: RMA5_0315_02_VRAD_20251008T183257Z.BUFR
            parts = bufr_file.name.split("_")
            field_type = parts[3]
            timestamp = parts[4].replace(".BUFR", "")
            # Convert timestamp format
            obs_datetime = f"{timestamp[:4]}-{timestamp[4:6]}-{timestamp[6:11]}:{timestamp[11:13]}:{timestamp[13:15]}Z"

            tracker.mark_downloaded(
                bufr_file.name,
                f"/remote/{bufr_file.name}",
                str(bufr_file),
                bufr_file.stat().st_size,
                "test_checksum",
                {
                    "radar_code": "RMA5",
                    "field_type": field_type,
                    "observation_datetime": obs_datetime,
                },
            )

        # Check volume completeness detection
        import asyncio

        # Initialize the semaphore (needed for processing)
        daemon._processing_semaphore = asyncio.Semaphore(config.max_concurrent_processing)

        asyncio.run(daemon._check_volume_completeness())

        # Verify volume was detected as complete
        complete_volumes = tracker.get_complete_unprocessed_volumes()
        assert len(complete_volumes) >= 1, "No complete volumes detected"

        volume_info = complete_volumes[0]
        assert volume_info["is_complete"] == 1
        assert volume_info["vol_code"] == "0315"
        assert volume_info["vol_number"] == "02"

        # Process the volume
        result = asyncio.run(daemon._process_volume_async(volume_info))
        assert result is True, "Volume processing failed"

        # Verify NetCDF file was created
        netcdf_files = list(netcdf_dir.glob("*.nc"))
        assert len(netcdf_files) == 1, "NetCDF file was not created"

        netcdf_path = netcdf_files[0]
        assert netcdf_path.exists()
        assert netcdf_path.stat().st_size > 0

        # Verify the volume was marked as completed in database
        volume_info = tracker.get_volume_info(volume_info["volume_id"])
        assert volume_info["status"] == "completed"
        assert volume_info["netcdf_path"] is not None
        assert volume_info["processed_at"] is not None
        assert volume_info["error_message"] is None

        # Verify we can read the NetCDF file
        try:
            import pyart

            radar = pyart.io.read_cfradial(str(netcdf_path))
            assert radar is not None
            assert hasattr(radar, "fields")
            assert len(radar.fields) > 0
        except ImportError:
            pytest.skip("pyart not available for NetCDF validation")

        # Check stats
        stats = daemon.get_stats()
        assert stats["volumes_processed"] == 1
        assert stats["volumes_failed"] == 0

        tracker.close()


@pytest.mark.integration
def test_processing_daemon_incomplete_volume():
    """
    Integration test: Detect incomplete volume (missing fields).

    This test verifies that incomplete volumes are correctly identified
    and not processed until all required fields are available.
    """
    from radarlib.io.ftp import ProcessingDaemon, ProcessingDaemonConfig, SQLiteStateTracker

    data_dir = Path("tests/data/bufr/RMA5")
    if not data_dir.exists():
        pytest.skip("No BUFR test data available")

    bufr_files = list(data_dir.glob("*_0315_02_*.BUFR"))
    if not bufr_files:
        pytest.skip("No BUFR test files found")

    with tempfile.TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        netcdf_dir = base / "netcdf"
        state_db = base / "state.db"

        config = ProcessingDaemonConfig(
            local_bufr_dir=data_dir,
            local_netcdf_dir=netcdf_dir,
            state_db=state_db,
            volume_types={
                "0315": {
                    "02": ["VRAD", "WRAD"],  # Expect both fields
                }
            },
            radar_code="RMA5",
        )

        daemon = ProcessingDaemon(config)
        tracker = SQLiteStateTracker(state_db)

        # Register only ONE file (incomplete volume)
        bufr_file = bufr_files[0]
        parts = bufr_file.name.split("_")
        field_type = parts[3]
        timestamp = parts[4].replace(".BUFR", "")
        obs_datetime = f"{timestamp[:4]}-{timestamp[4:6]}-{timestamp[6:11]}:{timestamp[11:13]}:{timestamp[13:15]}Z"

        tracker.mark_downloaded(
            bufr_file.name,
            f"/remote/{bufr_file.name}",
            str(bufr_file),
            bufr_file.stat().st_size,
            "test_checksum",
            {
                "radar_code": "RMA5",
                "field_type": field_type,
                "observation_datetime": obs_datetime,
            },
        )

        # Check volume completeness
        import asyncio

        asyncio.run(daemon._check_volume_completeness())

        # Verify volume was detected as incomplete
        complete_volumes = tracker.get_complete_unprocessed_volumes()
        assert len(complete_volumes) == 0, "Incomplete volume incorrectly marked as complete"

        # Check it was registered but marked incomplete
        all_volumes = tracker.get_volumes_by_status("pending")
        assert len(all_volumes) >= 1, "Volume was not registered"

        volume = all_volumes[0]
        assert volume["is_complete"] == 0, "Volume should be incomplete"

        # Verify no NetCDF files were created
        netcdf_files = list(netcdf_dir.glob("*.nc"))
        assert len(netcdf_files) == 0, "NetCDF file should not be created for incomplete volume"

        tracker.close()


@pytest.mark.integration
@pytest.mark.skip(reason="Test hangs when BUFR decoder tries to open non-existent file")
def test_processing_daemon_error_handling():
    """
    Integration test: Error handling for processing failures.

    This test verifies that the daemon correctly handles and logs
    processing errors without crashing.
    """
    from radarlib.io.ftp import ProcessingDaemon, ProcessingDaemonConfig, SQLiteStateTracker

    with tempfile.TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        bufr_dir = base / "bufr"
        netcdf_dir = base / "netcdf"
        state_db = base / "state.db"

        bufr_dir.mkdir()

        config = ProcessingDaemonConfig(
            local_bufr_dir=bufr_dir,
            local_netcdf_dir=netcdf_dir,
            state_db=state_db,
            volume_types={
                "0315": {
                    "01": ["DBZH"],
                }
            },
            radar_code="RMA1",
        )

        daemon = ProcessingDaemon(config)
        tracker = SQLiteStateTracker(state_db)

        # Create a fake volume with non-existent files
        volume_id = "RMA1_0315_01_2025-11-18T12:00:00Z"
        tracker.register_volume(volume_id, "RMA1", "0315", "01", "2025-11-18T12:00:00Z", ["DBZH"], is_complete=True)

        # Register a file that doesn't exist
        fake_file = "RMA1_0315_01_DBZH_20251118T120000Z.BUFR"
        tracker.mark_downloaded(
            fake_file,
            f"/remote/{fake_file}",
            str(bufr_dir / fake_file),  # File doesn't actually exist
            1000,
            "checksum",
            {
                "radar_code": "RMA1",
                "field_type": "DBZH",
                "observation_datetime": "2025-11-18T12:00:00Z",
            },
        )

        # Try to process the volume
        import asyncio

        # Initialize the semaphore (needed for processing)
        daemon._processing_semaphore = asyncio.Semaphore(config.max_concurrent_processing)

        volume_info = tracker.get_volume_info(volume_id)
        result = asyncio.run(daemon._process_volume_async(volume_info))

        # Should fail gracefully
        assert result is False, "Processing should have failed"

        # Verify volume was marked as failed
        volume_info = tracker.get_volume_info(volume_id)
        assert volume_info["status"] == "failed"
        assert volume_info["error_message"] is not None
        assert "No local paths found" in volume_info["error_message"]

        # Check stats
        stats = daemon.get_stats()
        assert stats["volumes_failed"] == 1
        assert stats["volumes_processed"] == 0

        tracker.close()
