#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Example: FTP Daemon Service for BUFR Files

This example demonstrates how to set up and run the FTP daemon service
for continuously monitoring and downloading new BUFR files.
"""

import asyncio
import signal
from pathlib import Path

from radarlib import config
from radarlib.io.ftp import FTPDaemon, FTPDaemonConfig


def main():
    """Main function to run the FTP daemon."""
    print("=" * 60)
    print("FTP Daemon Example - Continuous BUFR File Monitoring")
    print("=" * 60)

    # Configure the daemon
    daemon_config = FTPDaemonConfig(
        host=config.FTP_HOST,
        username=config.FTP_USER,
        password=config.FTP_PASSWORD,
        remote_base_path="/L2/RMA1",  # Monitor this directory
        local_download_dir=Path("./daemon_downloads"),
        state_file=Path("./daemon_state.json"),
        poll_interval=60,  # Check every 60 seconds
        max_concurrent_downloads=3,  # Download up to 3 files simultaneously
    )

    # Create daemon instance
    daemon = FTPDaemon(daemon_config)

    print("\nDaemon Configuration:")
    print(f"  FTP Host: {daemon_config.host}")
    print(f"  Remote Path: {daemon_config.remote_base_path}")
    print(f"  Local Dir: {daemon_config.local_download_dir}")
    print(f"  Poll Interval: {daemon_config.poll_interval}s")
    print(f"  Max Concurrent: {daemon_config.max_concurrent_downloads}")
    print(f"  State File: {daemon_config.state_file}")

    print("\nStarting daemon...")
    print("Press Ctrl+C to stop\n")
    print("-" * 60)

    # Setup signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        print("\nReceived shutdown signal. Stopping daemon...")
        daemon.stop()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Run the daemon
    try:
        asyncio.run(daemon.run())
    except KeyboardInterrupt:
        print("\nDaemon stopped by user")
    except Exception as e:
        print(f"\nDaemon error: {e}")
    finally:
        stats = daemon.get_stats()
        print("\n" + "=" * 60)
        print("Daemon Statistics:")
        print(f"  Total files downloaded: {stats['total_downloaded']}")
        print("=" * 60)


def check_status_example():
    """
    Example: Check daemon status without running it.

    This shows how to inspect the daemon's state file to see
    what has been downloaded.
    """
    print("=" * 60)
    print("Check Daemon Status Example")
    print("=" * 60)

    from radarlib.io.ftp import FileStateTracker

    # Load the state tracker
    state_file = Path("./daemon_state.json")
    if not state_file.exists():
        print(f"\nNo state file found at {state_file}")
        print("The daemon has not run yet or has no downloads.")
        return

    tracker = FileStateTracker(state_file)

    print(f"\nState File: {state_file}")
    print(f"Total Files Downloaded: {tracker.count()}")

    # Show some recent downloads
    downloaded_files = tracker.get_downloaded_files()
    print("\nRecent downloads (showing first 10):")
    for i, filename in enumerate(list(downloaded_files)[:10], 1):
        info = tracker.get_file_info(filename)
        print(f"  {i}. {filename}")
        print(f"     Downloaded: {info['downloaded_at']}")
        print(f"     Remote: {info['remote_path']}")

    print("\n" + "=" * 60)


def manual_cycle_example():
    """
    Example: Run a single check cycle manually.

    This is useful for testing or running the check on-demand
    instead of continuously.
    """
    print("=" * 60)
    print("Manual Check Cycle Example")
    print("=" * 60)

    daemon_config = FTPDaemonConfig(
        host=config.FTP_HOST,
        username=config.FTP_USER,
        password=config.FTP_PASSWORD,
        remote_base_path="/L2/RMA1",
        local_download_dir=Path("./manual_downloads"),
        state_file=Path("./manual_state.json"),
    )

    daemon = FTPDaemon(daemon_config)
    daemon._download_semaphore = asyncio.Semaphore(daemon_config.max_concurrent_downloads)

    print("\nRunning single check cycle...")

    async def run_once():
        await daemon._check_and_download_new_files()
        stats = daemon.get_stats()
        print("\nCheck complete!")
        print(f"Total files tracked: {stats['total_downloaded']}")

    asyncio.run(run_once())

    print("\n" + "=" * 60)


if __name__ == "__main__":
    # Uncomment the example you want to run:

    # Run the daemon continuously
    main()

    # Check daemon status
    # check_status_example()

    # Run a single manual check
    # manual_cycle_example()
