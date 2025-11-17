"""
Example 1: Basic async FTP client usage.

This example demonstrates how to use the AsyncFTPClient to connect to an FTP server,
list files, and download them.
"""

import asyncio
import logging
from pathlib import Path

from radarlib.io.ftp import AsyncFTPClient

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


async def example_basic_client():
    """
    Example 1: Basic FTP client operations.

    This example shows:
    1. Connecting to an FTP server
    2. Listing files in a directory
    3. Downloading a single file
    4. Downloading multiple files concurrently
    """
    logger.info("=" * 80)
    logger.info("Example 1: Basic Async FTP Client Usage")
    logger.info("=" * 80)

    # FTP server configuration
    # Replace with your actual FTP server details
    host = "ftp.example.com"
    username = "your_username"
    password = "your_password"
    remote_path = "/radar/data"
    local_dir = Path("/tmp/radar_downloads")

    # Ensure local directory exists
    local_dir.mkdir(parents=True, exist_ok=True)

    # Use context manager for automatic connection/disconnection
    async with AsyncFTPClient(host, username, password) as client:
        # List files in remote directory
        logger.info(f"\nListing BUFR files in {remote_path}...")
        files = await client.list_files(remote_path, pattern="*.BUFR")

        logger.info(f"Found {len(files)} BUFR files:")
        for i, file in enumerate(files[:5], 1):  # Show first 5 files
            logger.info(f"  {i}. {file}")

        if len(files) > 5:
            logger.info(f"  ... and {len(files) - 5} more files")

        # Download first file (if any)
        if files:
            logger.info("\nDownloading first file...")
            local_file = await client.download_file(files[0], str(local_dir))
            logger.info(f"Downloaded to: {local_file}")

            # Download multiple files concurrently (up to 3 files)
            if len(files) > 1:
                logger.info("\nDownloading up to 3 files concurrently...")
                files_to_download = files[1:4]  # Get next 3 files
                downloaded = await client.download_files(files_to_download, str(local_dir), max_concurrent=3)

                logger.info(f"Successfully downloaded {len(downloaded)} files:")
                for local_path in downloaded:
                    logger.info(f"  - {Path(local_path).name}")

    logger.info("\n✓ Example completed successfully!")


async def example_manual_connection():
    """
    Example 2: Manual connection management.

    This example shows how to manage FTP connection manually without context manager.
    """
    logger.info("\n" + "=" * 80)
    logger.info("Example 2: Manual Connection Management")
    logger.info("=" * 80)

    # Create client instance
    client = AsyncFTPClient(host="ftp.example.com", username="your_username", password="your_password", logger=logger)

    try:
        # Connect manually
        await client.connect()

        # List files
        files = await client.list_files("/radar/data", pattern="*.BUFR")
        logger.info(f"Found {len(files)} files")

        # Check if a specific file exists
        if files:
            exists = await client.check_file_exists(files[0])
            logger.info(f"File {files[0]} exists: {exists}")

    finally:
        # Always disconnect
        await client.disconnect()

    logger.info("\n✓ Example completed successfully!")


async def example_with_error_handling():
    """
    Example 3: Error handling and retries.

    This example demonstrates robust error handling when working with FTP servers.
    """
    logger.info("\n" + "=" * 80)
    logger.info("Example 3: Error Handling and Retries")
    logger.info("=" * 80)

    async def download_with_retry(client, remote_path, local_dir, max_retries=3):
        """Download a file with retry logic."""
        for attempt in range(max_retries):
            try:
                logger.info(f"Attempt {attempt + 1}/{max_retries} to download {remote_path}")
                local_path = await client.download_file(remote_path, local_dir)
                logger.info(f"Successfully downloaded on attempt {attempt + 1}")
                return local_path
            except Exception as e:
                logger.warning(f"Download failed on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2**attempt)  # Exponential backoff
                else:
                    logger.error(f"Failed to download after {max_retries} attempts")
                    raise

    try:
        async with AsyncFTPClient("ftp.example.com", "user", "pass") as client:
            files = await client.list_files("/radar/data", pattern="*.BUFR")

            if files:
                # Try to download with retry logic
                await download_with_retry(client, files[0], "/tmp/radar_downloads")

    except Exception as e:
        logger.error(f"Error in example: {e}")

    logger.info("\n✓ Example completed!")


if __name__ == "__main__":
    logger.info("\n")
    logger.info("╔" + "=" * 78 + "╗")
    logger.info("║" + "  Async FTP Client Examples".center(78) + "║")
    logger.info("║" + "  for Radar BUFR File Downloads".center(78) + "║")
    logger.info("╚" + "=" * 78 + "╝")
    logger.info("\n")

    # Run examples
    try:
        # NOTE: Replace FTP credentials with actual values to run these examples
        logger.info("NOTE: Update FTP credentials in the code before running!\n")

        # Uncomment to run examples:
        # asyncio.run(example_basic_client())
        # asyncio.run(example_manual_connection())
        # asyncio.run(example_with_error_handling())

        logger.info("Examples are ready to run after updating FTP credentials.")

    except KeyboardInterrupt:
        logger.info("\nExamples interrupted by user")
    except Exception as e:
        logger.error(f"Error running examples: {e}", exc_info=True)
