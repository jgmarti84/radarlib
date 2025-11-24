#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Example: Date-Based FTP Daemon for BUFR Files (Legacy)

This example demonstrates how to use the DateBasedFTPDaemon for monitoring
and downloading BUFR files based on date ranges with SQLite state tracking.

Note: For new projects, consider using DownloadDaemon instead.
"""

import asyncio
import os
from datetime import datetime, timezone
from pathlib import Path

from radarlib import config
from radarlib.daemons.legacy import DateBasedDaemonConfig, DateBasedFTPDaemon


def example_ongoing_daemon_with_voltypes_filering():
    """
    Example: Ongoing daemon with volume types filtering.

    This will keep running and downloading new files as they appear,
    but only for specified volume types and field types.
    """
    print("=" * 60)
    print("Ongoing Daemon with Volume Types Filtering Example")
    print("=" * 60)

    # Define valid volume types for radar
    # Format: {vol_code: {vol_number: [field_types]}}
    volume_types = {
        "0315": {
            "01": ["DBZH", "DBZV", "ZDR", "RHOHV", "PHIDP", "KDP"],
            "02": ["VRAD", "WRAD"],
        },
    }
    radar_name = "RMA1"
    daemon_config = DateBasedDaemonConfig(
        host=config.FTP_HOST,
        username=config.FTP_USER,
        password=config.FTP_PASS,
        remote_base_path="/L2",
        radar_code=radar_name,
        local_download_dir=Path(os.path.join(config.ROOT_RADAR_FILES_PATH, radar_name, "bufr")),
        state_db=Path(os.path.join(config.ROOT_RADAR_FILES_PATH, radar_name, "state.db")),
        start_date=datetime(2025, 11, 17, tzinfo=timezone.utc),
        end_date=None,  # No end date - runs indefinitely
        poll_interval=60,
        volume_types=volume_types,  # Apply filtering
    )
    daemon = DateBasedFTPDaemon(daemon_config)
    print("\nStarting ongoing daemon with volume types filtering...")
    print("-" * 60)
    try:
        asyncio.run(daemon.run())
    except KeyboardInterrupt:
        print("\nDaemon stopped by user")

    stats = daemon.get_stats()
    print("\n" + "=" * 60)
    print("Daemon Statistics:")
    print(f"  Total files downloaded: {stats['total_downloaded']}")
    print(f"  Current scan date: {stats['current_scan_date']}")
    print("=" * 60)


def example_date_range_daemon():
    """
    Example: Download BUFR files for a specific date range.

    This will scan all hourly directories between start and end dates,
    download new files, and automatically stop when complete.
    """
    print("=" * 60)
    print("Date-Based FTP Daemon Example")
    print("=" * 60)

    # Configure for specific date range
    daemon_config = DateBasedDaemonConfig(
        host=config.FTP_HOST,
        username=config.FTP_USER,
        password=config.FTP_PASS,
        remote_base_path="/L2",
        radar_code="RMA1",  # Specific radar to monitor
        local_download_dir=Path("./date_downloads"),
        state_db=Path("./date_daemon_state.db"),
        start_date=datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc),
        end_date=datetime(2025, 1, 2, 0, 0, tzinfo=timezone.utc),  # Will stop after this
        poll_interval=30,  # Check every 30 seconds
        max_concurrent_downloads=3,
        verify_checksums=True,  # Calculate SHA256 checksums
        resume_partial=True,  # Resume interrupted downloads
    )

    daemon = DateBasedFTPDaemon(daemon_config)

    print("\nDaemon Configuration:")
    print(f"  Radar: {daemon_config.radar_code}")
    print(f"  Start: {daemon_config.start_date}")
    print(f"  End: {daemon_config.end_date}")
    print(f"  Local Dir: {daemon_config.local_download_dir}")
    print(f"  State DB: {daemon_config.state_db}")
    print(f"  Verify Checksums: {daemon_config.verify_checksums}")

    print("\nStarting daemon (will auto-stop when date range complete)...")
    print("-" * 60)

    try:
        asyncio.run(daemon.run())
    except KeyboardInterrupt:
        print("\nDaemon stopped by user")

    stats = daemon.get_stats()
    print("\n" + "=" * 60)
    print("Daemon Statistics:")
    print(f"  Total files downloaded: {stats['total_downloaded']}")
    print(f"  Current scan date: {stats['current_scan_date']}")
    print("=" * 60)


def example_ongoing_monitoring():
    """
    Example: Continuous monitoring without end date.

    This will keep running and downloading new files as they appear.
    """
    print("=" * 60)
    print("Ongoing Monitoring Example")
    print("=" * 60)

    daemon_config = DateBasedDaemonConfig(
        host=config.FTP_HOST,
        username=config.FTP_USER,
        password=config.FTP_PASS,
        remote_base_path="/L2",
        radar_code="RMA1",
        local_download_dir=Path("./ongoing_downloads"),
        state_db=Path("./ongoing_state.db"),
        start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
        end_date=None,  # No end date - runs indefinitely
        poll_interval=60,
    )

    daemon = DateBasedFTPDaemon(daemon_config)

    print("\nStarting continuous monitoring...")
    print("Press Ctrl+C to stop\n")
    print("-" * 60)

    try:
        asyncio.run(daemon.run())
    except KeyboardInterrupt:
        print("\nDaemon stopped")

    print("\n" + "=" * 60)


def example_resume_download():
    """
    Example: Resume downloading after interruption.

    The daemon automatically resumes from the last successfully
    downloaded file based on the SQLite database state.
    """
    print("=" * 60)
    print("Resume Download Example")
    print("=" * 60)

    daemon_config = DateBasedDaemonConfig(
        host=config.FTP_HOST,
        username=config.FTP_USER,
        password=config.FTP_PASS,
        remote_base_path="/L2",
        radar_code="RMA1",
        local_download_dir=Path("./resume_downloads"),
        state_db=Path("./resume_state.db"),
        start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
        end_date=datetime(2025, 1, 10, tzinfo=timezone.utc),
    )

    daemon = DateBasedFTPDaemon(daemon_config)

    print("\nDaemon will resume from last download in database")
    print("If interrupted, restart with same config to continue")
    print("\nPress Ctrl+C to stop\n")
    print("-" * 60)

    try:
        asyncio.run(daemon.run())
    except KeyboardInterrupt:
        print("\nDaemon stopped - progress saved to database")
        print("Restart to resume from last downloaded file")

    stats = daemon.get_stats()
    print("\n" + "=" * 60)
    print(f"Downloaded {stats['total_downloaded']} files")
    print(f"Last scan date: {stats['current_scan_date']}")
    print("=" * 60)


def example_check_database_stats():
    """
    Example: Query the SQLite database for statistics.

    Shows how to inspect what has been downloaded without running the daemon.
    """
    print("=" * 60)
    print("Database Statistics Example")
    print("=" * 60)

    from radarlib.io.ftp import SQLiteStateTracker

    db_path = Path("./date_daemon_state.db")

    if not db_path.exists():
        print(f"\nNo database found at {db_path}")
        print("Run the daemon first to create the database.")
        return

    tracker = SQLiteStateTracker(db_path)

    print(f"\nDatabase: {db_path}")
    print(f"Total files downloaded: {tracker.count()}")

    # Get files for a specific date range
    files = tracker.get_files_by_date_range(
        datetime(2025, 1, 1, tzinfo=timezone.utc),
        datetime(2025, 1, 2, tzinfo=timezone.utc),
        radar_code="RMA1",
    )

    print(f"\nFiles in date range: {len(files)}")

    # Show some file details
    if files:
        print("\nSample files (first 5):")
        for i, filename in enumerate(files[:5], 1):
            info = tracker.get_file_info(filename)
            if info:
                print(f"\n  {i}. {filename}")
                print(f"     Downloaded: {info['downloaded_at']}")
                print(f"     Size: {info['file_size']} bytes")
                print(f"     Checksum: {info['checksum'][:16]}..." if info["checksum"] else "     Checksum: None")
                print(f"     Field: {info['field_type']}")

    tracker.close()
    print("\n" + "=" * 60)


def example_multiple_radars():
    """
    Example: Run multiple daemons for different radars.

    Shows how to monitor multiple radars concurrently.
    """
    print("=" * 60)
    print("Multiple Radars Example")
    print("=" * 60)

    async def run_multiple_daemons():
        radars = ["RMA1", "RMA5", "AR5"]
        tasks = []

        for radar in radars:
            config_obj = DateBasedDaemonConfig(
                host=config.FTP_HOST,
                username=config.FTP_USER,
                password=config.FTP_PASS,
                remote_base_path="/L2",
                radar_code=radar,
                local_download_dir=Path(f"./downloads_{radar}"),
                state_db=Path(f"./state_{radar}.db"),
                start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
                end_date=datetime(2025, 1, 2, tzinfo=timezone.utc),
            )

            daemon = DateBasedFTPDaemon(config_obj)
            tasks.append(daemon.run())

        print(f"\nStarting daemons for {len(radars)} radars...")
        print("All will run concurrently")
        print("\nPress Ctrl+C to stop all\n")
        print("-" * 60)

        await asyncio.gather(*tasks)

    try:
        asyncio.run(run_multiple_daemons())
    except KeyboardInterrupt:
        print("\nAll daemons stopped")

    print("\n" + "=" * 60)


def example_volume_filtering():
    """
    Example: Download only specific volume types and field types.

    This shows how to filter BUFR files based on volume code, volume number,
    and field types using the volume_types configuration.
    """
    print("=" * 60)
    print("Volume Filtering Example")
    print("=" * 60)

    # Define valid volume types for radar
    # Format: {vol_code: {vol_number: [field_types]}}
    volume_types = {
        "0315": {
            "01": ["DBZH", "DBZV", "ZDR", "RHOHV", "PHIDP", "KDP"],
            "02": ["VRAD", "WRAD"],
        },
        "0516": {
            "01": ["DBZH", "DBZV"],
            "02": ["VRAD"],
        },
    }

    daemon_config = DateBasedDaemonConfig(
        host=config.FTP_HOST,
        username=config.FTP_USER,
        password=config.FTP_PASS,
        remote_base_path="/L2",
        radar_code="RMA1",
        local_download_dir=Path("./filtered_downloads"),
        state_db=Path("./filtered_state.db"),
        start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
        end_date=datetime(2025, 1, 2, tzinfo=timezone.utc),
        volume_types=volume_types,  # Apply filtering
    )

    daemon = DateBasedFTPDaemon(daemon_config)

    print("\nDaemon Configuration:")
    print(f"  Radar: {daemon_config.radar_code}")
    print("  Volume Filtering: Enabled")
    print("  Configured volume types:")
    for vol_code, vol_nums in volume_types.items():
        print(f"    Volume {vol_code}:")
        for vol_num, fields in vol_nums.items():
            print(f"      Vol #{vol_num}: {', '.join(fields)}")

    print("\nStarting daemon with volume filtering...")
    print("Only files matching volume_types will be downloaded")
    print("-" * 60)

    try:
        asyncio.run(daemon.run())
    except KeyboardInterrupt:
        print("\nDaemon stopped by user")

    stats = daemon.get_stats()
    print("\n" + "=" * 60)
    print("Daemon Statistics:")
    print(f"  Total files downloaded: {stats['total_downloaded']}")
    print("=" * 60)


if __name__ == "__main__":
    # Uncomment the example you want to run:

    # example_ongoing_daemon_with_voltypes_filering()
    example_ongoing_daemon_with_voltypes_filering()

    # Download files for a specific date range (auto-stops when complete)
    # example_date_range_daemon()

    # Continuous monitoring without end date
    # example_ongoing_monitoring()

    # Resume after interruption
    # example_resume_download()

    # Check database statistics
    # example_check_database_stats()

    # Monitor multiple radars concurrently
    # example_multiple_radars()

    # Download with volume filtering
    # example_volume_filtering()

    # Monitor multiple radars concurrently
    # example_multiple_radars()
