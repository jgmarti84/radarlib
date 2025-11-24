#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Example: BUFR Processing Daemon

This example demonstrates how to use the ProcessingDaemon to monitor
downloaded BUFR files and automatically process complete volumes into
NetCDF files.
"""

import asyncio
import os
from datetime import datetime, timezone
from pathlib import Path

from radarlib import config
from radarlib.daemons import ProcessingDaemon, ProcessingDaemonConfig


def example_basic_processing_daemon():
    """
    Example: Basic processing daemon setup.

    This daemon will monitor BUFR files downloaded by the DateBasedFTPDaemon
    and process complete volumes automatically.
    """
    print("=" * 60)
    print("Basic BUFR Processing Daemon Example")
    print("=" * 60)

    # Define volume types - same as used in download daemon
    volume_types = {
        "0315": {
            "01": ["DBZH", "DBZV", "ZDR", "RHOHV", "PHIDP", "KDP"],
            "02": ["VRAD", "WRAD"],
        },
    }

    radar_name = "RMA1"
    base_path = Path(os.path.join(config.ROOT_RADAR_FILES_PATH, radar_name))

    daemon_config = ProcessingDaemonConfig(
        local_bufr_dir=base_path / "bufr",  # Where BUFR files are downloaded
        local_netcdf_dir=base_path / "netcdf",  # Where to save NetCDF files
        state_db=base_path / "state.db",  # Same database as download daemon
        volume_types=volume_types,
        radar_name=radar_name,
        start_date=datetime(2025, 11, 23, 20, 0, 0, tzinfo=timezone.utc),
        poll_interval=30,  # Check every 30 seconds
        max_concurrent_processing=2,  # Process 2 volumes at a time
    )

    daemon = ProcessingDaemon(daemon_config)

    print("\nDaemon Configuration:")
    print(f"  Radar: {daemon_config.radar_name}")
    print(f"  BUFR Dir: {daemon_config.local_bufr_dir}")
    print(f"  NetCDF Dir: {daemon_config.local_netcdf_dir}")
    print(f"  State DB: {daemon_config.state_db}")
    print(f"  Poll Interval: {daemon_config.poll_interval}s")

    print("\nStarting processing daemon...")
    print("Press Ctrl+C to stop\n")
    print("-" * 60)

    try:
        asyncio.run(daemon.run())
    except KeyboardInterrupt:
        print("\nDaemon stopped by user")

    stats = daemon.get_stats()
    print("\n" + "=" * 60)
    print("Daemon Statistics:")
    print(f"  Volumes processed: {stats['volumes_processed']}")
    print(f"  Volumes failed: {stats['volumes_failed']}")
    print(f"  Incomplete volumes detected: {stats['incomplete_volumes_detected']}")
    print(f"  Pending volumes: {stats['pending_volumes']}")
    print("=" * 60)


def example_combined_download_and_processing():
    """
    Example: Run download and processing daemons together.

    This shows how to run both daemons concurrently to create a
    complete pipeline from FTP download to NetCDF generation.
    """
    print("=" * 60)
    print("Combined Download and Processing Daemon Example")
    print("=" * 60)

    from radarlib.io.ftp import DateBasedDaemonConfig, DateBasedFTPDaemon

    volume_types = {
        "0315": {
            "01": ["DBZH", "DBZV", "ZDR", "RHOHV", "PHIDP", "KDP"],
            "02": ["VRAD", "WRAD"],
        },
    }

    radar_name = "RMA1"
    base_path = Path(os.path.join(config.ROOT_RADAR_FILES_PATH, radar_name))

    # Configure download daemon
    download_config = DateBasedDaemonConfig(
        host=config.FTP_HOST,
        username=config.FTP_USER,
        password=config.FTP_PASS,
        remote_base_path="/L2",
        radar_code=radar_name,
        local_download_dir=base_path / "bufr",
        state_db=base_path / "state.db",
        start_date=datetime(2025, 11, 17, tzinfo=timezone.utc),
        end_date=None,  # Continuous monitoring
        poll_interval=60,
        volume_types=volume_types,
    )

    # Configure processing daemon
    processing_config = ProcessingDaemonConfig(
        local_bufr_dir=base_path / "bufr",
        local_netcdf_dir=base_path / "netcdf",
        state_db=base_path / "state.db",
        volume_types=volume_types,
        radar_name=radar_name,
        poll_interval=30,
    )

    download_daemon = DateBasedFTPDaemon(download_config)
    processing_daemon = ProcessingDaemon(processing_config)

    async def run_both_daemons():
        """Run both daemons concurrently."""
        print("\nStarting both daemons concurrently...")
        print("  - Download daemon: monitoring FTP for new BUFR files")
        print("  - Processing daemon: processing complete volumes to NetCDF")
        print("\nPress Ctrl+C to stop both daemons\n")
        print("-" * 60)

        tasks = [
            asyncio.create_task(download_daemon.run()),
            asyncio.create_task(processing_daemon.run()),
        ]

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            print("\nBoth daemons cancelled")

    try:
        asyncio.run(run_both_daemons())
    except KeyboardInterrupt:
        print("\nDaemons stopped by user")

    # Show statistics from both daemons
    download_stats = download_daemon.get_stats()
    processing_stats = processing_daemon.get_stats()

    print("\n" + "=" * 60)
    print("Download Daemon Statistics:")
    print(f"  Total files downloaded: {download_stats['total_downloaded']}")
    print(f"  Current scan date: {download_stats['current_scan_date']}")

    print("\nProcessing Daemon Statistics:")
    print(f"  Volumes processed: {processing_stats['volumes_processed']}")
    print(f"  Volumes failed: {processing_stats['volumes_failed']}")
    print(f"  Complete unprocessed: {processing_stats['complete_unprocessed']}")
    print("=" * 60)


def example_check_processing_status():
    """
    Example: Check volume processing status.

    Shows how to query the database to see which volumes have been
    processed, which are pending, and which failed.
    """
    print("=" * 60)
    print("Volume Processing Status Example")
    print("=" * 60)

    from radarlib.io.ftp import SQLiteStateTracker

    radar_name = "RMA1"
    db_path = Path(os.path.join(config.ROOT_RADAR_FILES_PATH, radar_name, "state.db"))

    if not db_path.exists():
        print(f"\nNo database found at {db_path}")
        print("Run the processing daemon first to create the database.")
        return

    tracker = SQLiteStateTracker(db_path)

    print(f"\nDatabase: {db_path}")

    # Get volumes by status
    pending = tracker.get_volumes_by_status("pending")
    processing = tracker.get_volumes_by_status("processing")
    completed = tracker.get_volumes_by_status("completed")
    failed = tracker.get_volumes_by_status("failed")

    print("\nVolume Status Summary:")
    print(f"  Pending: {len(pending)}")
    print(f"  Processing: {len(processing)}")
    print(f"  Completed: {len(completed)}")
    print(f"  Failed: {len(failed)}")

    # Show complete unprocessed volumes
    complete_unprocessed = tracker.get_complete_unprocessed_volumes()
    print(f"\nComplete volumes ready for processing: {len(complete_unprocessed)}")

    if complete_unprocessed:
        print("\nNext volumes to process:")
        for i, vol in enumerate(complete_unprocessed[:5], 1):
            print(f"\n  {i}. {vol['volume_id']}")
            print(f"     Radar: {vol['radar_name']}")
            print(f"     Vol: {vol['strategy']}/{vol['vol_nr']}")
            print(f"     Time: {vol['observation_datetime']}")
            print(f"     Fields: {vol['expected_fields']}")

    # Show completed volumes
    if completed:
        print("\nRecently completed volumes (last 5):")
        for i, vol in enumerate(completed[-5:], 1):
            print(f"\n  {i}. {vol['volume_id']}")
            print(f"     NetCDF: {vol['netcdf_path']}")
            print(f"     Processed: {vol['processed_at']}")

    # Show failed volumes
    if failed:
        print("\nFailed volumes:")
        for i, vol in enumerate(failed, 1):
            print(f"\n  {i}. {vol['volume_id']}")
            print(f"     Error: {vol['error_message']}")

    tracker.close()
    print("\n" + "=" * 60)


if __name__ == "__main__":
    # Uncomment the example you want to run:

    # Basic processing daemon
    example_basic_processing_daemon()

    # Combined download and processing pipeline
    # example_combined_download_and_processing()

    # Check processing status
    # example_check_processing_status()
