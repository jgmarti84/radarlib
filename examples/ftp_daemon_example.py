"""
Example 2: FTP Daemon service usage.

This example demonstrates how to use the FTPDaemon to set up a background service
that continuously monitors an FTP server for new BUFR files.
"""

import asyncio
import logging
from pathlib import Path

from radarlib.io.ftp import FTPDaemon, FTPDaemonConfig

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


async def example_basic_daemon():
    """
    Example 1: Basic daemon service.

    This example shows how to set up a daemon that monitors an FTP server
    and downloads new BUFR files as they appear.
    """
    logger.info("=" * 80)
    logger.info("Example 1: Basic FTP Daemon Service")
    logger.info("=" * 80)

    # Configure the daemon
    config = FTPDaemonConfig(
        host="ftp.example.com",
        username="your_username",
        password="your_password",
        remote_path="/radar/data",
        local_dir="/tmp/radar_daemon",
        port=21,
        file_pattern="*.BUFR",
        poll_interval=60,  # Check every 60 seconds
        max_concurrent_downloads=5,
        recursive=False,
    )

    # Create daemon instance
    daemon = FTPDaemon(config, logger=logger)

    # For demonstration, run for only 3 iterations
    # In production, you would not set max_iterations (runs indefinitely)
    logger.info("Starting daemon (will run for 3 iterations)...")
    await daemon.run(max_iterations=3)

    logger.info("\n✓ Example completed successfully!")


async def example_daemon_with_callback():
    """
    Example 2: Daemon with file processing callback.

    This example shows how to process files immediately after they are downloaded
    using a callback function.
    """
    logger.info("\n" + "=" * 80)
    logger.info("Example 2: Daemon with File Processing Callback")
    logger.info("=" * 80)

    # Define a callback to process downloaded files
    def process_file(local_path: str):
        """Process a newly downloaded file."""
        logger.info(f"📁 Processing new file: {Path(local_path).name}")

        # Example: You could decode the BUFR file here
        # from radarlib.io.bufr import bufr_to_dict
        # bufr_dict = bufr_to_dict(local_path)
        # ... process the data ...

        # For now, just log the file size
        file_size = Path(local_path).stat().st_size
        logger.info(f"   File size: {file_size / 1024:.2f} KB")
        logger.info(f"   ✓ Processing complete for {Path(local_path).name}")

    # Configure the daemon
    config = FTPDaemonConfig(
        host="ftp.example.com",
        username="your_username",
        password="your_password",
        remote_path="/radar/data",
        local_dir="/tmp/radar_with_callback",
        poll_interval=30,  # Check every 30 seconds
        max_concurrent_downloads=3,
    )

    # Create daemon with callback
    daemon = FTPDaemon(config, on_file_downloaded=process_file, logger=logger)

    # Run for 2 iterations
    logger.info("Starting daemon with callback (will run for 2 iterations)...")
    await daemon.run(max_iterations=2)

    # Show processed files
    processed = daemon.get_processed_files()
    logger.info(f"\nProcessed {len(processed)} files in total:")
    for filename in processed[:5]:  # Show first 5
        logger.info(f"  - {filename}")

    logger.info("\n✓ Example completed successfully!")


async def example_daemon_run_once():
    """
    Example 3: One-time download operation.

    This example shows how to use the daemon for a single download operation
    instead of continuous monitoring.
    """
    logger.info("\n" + "=" * 80)
    logger.info("Example 3: One-Time Download with Daemon")
    logger.info("=" * 80)

    config = FTPDaemonConfig(
        host="ftp.example.com",
        username="your_username",
        password="your_password",
        remote_path="/radar/data",
        local_dir="/tmp/radar_once",
        file_pattern="*.BUFR",
    )

    daemon = FTPDaemon(config, logger=logger)

    # Run once to download all available files
    logger.info("Running one-time download...")
    downloaded_count = await daemon.run_once()

    logger.info(f"Downloaded {downloaded_count} files")
    logger.info("\n✓ Example completed successfully!")


async def example_daemon_with_graceful_shutdown():
    """
    Example 4: Daemon with graceful shutdown handling.

    This example demonstrates how to handle graceful shutdown of the daemon service.
    """
    logger.info("\n" + "=" * 80)
    logger.info("Example 4: Daemon with Graceful Shutdown")
    logger.info("=" * 80)

    config = FTPDaemonConfig(
        host="ftp.example.com",
        username="your_username",
        password="your_password",
        remote_path="/radar/data",
        local_dir="/tmp/radar_shutdown",
        poll_interval=10,
    )

    daemon = FTPDaemon(config, logger=logger)

    # Create a task for the daemon
    daemon_task = asyncio.create_task(daemon.run())

    # Simulate running for a short time, then stopping
    try:
        await asyncio.sleep(5)  # Let it run for 5 seconds
        logger.info("\nInitiating graceful shutdown...")
        daemon.stop()  # Signal the daemon to stop
        await daemon_task  # Wait for it to finish
    except asyncio.CancelledError:
        logger.info("Daemon task was cancelled")

    logger.info("\n✓ Example completed successfully!")


async def example_production_daemon():
    """
    Example 5: Production-ready daemon setup.

    This example shows a more complete production setup with:
    - Proper error handling
    - File processing
    - Monitoring
    """
    logger.info("\n" + "=" * 80)
    logger.info("Example 5: Production-Ready Daemon")
    logger.info("=" * 80)

    # File processing callback
    def process_bufr_file(local_path: str):
        """Process downloaded BUFR file."""
        try:
            logger.info(f"Processing: {Path(local_path).name}")

            # In production, you would decode and process the BUFR file:
            # from radarlib.io.bufr import bufr_to_dict, bufr_to_pyart
            # bufr_dict = bufr_to_dict(local_path)
            # radar = bufr_to_pyart([bufr_dict])
            # ... generate products, store in database, etc.

            logger.info(f"✓ Processed: {Path(local_path).name}")

        except Exception as e:
            logger.error(f"Error processing {local_path}: {e}", exc_info=True)

    # Production configuration
    config = FTPDaemonConfig(
        host="ftp.example.com",
        username="your_username",
        password="your_password",
        remote_path="/radar/data",
        local_dir="/data/radar/incoming",
        port=21,
        file_pattern="*.BUFR",
        poll_interval=30,  # Check every 30 seconds
        max_concurrent_downloads=10,  # Download up to 10 files at once
        recursive=False,
    )

    # Create daemon with callback
    daemon = FTPDaemon(config, on_file_downloaded=process_bufr_file, logger=logger)

    # In production, this would run indefinitely
    # For demo, run for just 1 iteration
    logger.info("Starting production daemon (demo mode - 1 iteration)...")
    try:
        await daemon.run(max_iterations=1)
    except KeyboardInterrupt:
        logger.info("Daemon interrupted by user")
        daemon.stop()
    except Exception as e:
        logger.error(f"Fatal error in daemon: {e}", exc_info=True)
        raise

    logger.info("\n✓ Example completed successfully!")


if __name__ == "__main__":
    logger.info("\n")
    logger.info("╔" + "=" * 78 + "╗")
    logger.info("║" + "  FTP Daemon Service Examples".center(78) + "║")
    logger.info("║" + "  for Continuous Radar Data Monitoring".center(78) + "║")
    logger.info("╚" + "=" * 78 + "╝")
    logger.info("\n")

    # Run examples
    try:
        # NOTE: Replace FTP credentials with actual values to run these examples
        logger.info("NOTE: Update FTP credentials in the code before running!\n")

        # Uncomment to run examples:
        # asyncio.run(example_basic_daemon())
        # asyncio.run(example_daemon_with_callback())
        # asyncio.run(example_daemon_run_once())
        # asyncio.run(example_daemon_with_graceful_shutdown())
        # asyncio.run(example_production_daemon())

        logger.info("Examples are ready to run after updating FTP credentials.")

    except KeyboardInterrupt:
        logger.info("\nExamples interrupted by user")
    except Exception as e:
        logger.error(f"Error running examples: {e}", exc_info=True)
