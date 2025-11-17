# -*- coding: utf-8 -*-
"""Daemon service for continuously monitoring and downloading BUFR files."""

import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Set

from .client import FTPClient
from .state_tracker import FileStateTracker

logger = logging.getLogger(__name__)


@dataclass
class FTPDaemonConfig:
    """
    Configuration for the FTP daemon service.

    Attributes:
        host: FTP server hostname or IP
        username: FTP username
        password: FTP password
        remote_base_path: Base path on FTP server to monitor (e.g., "/L2")
        local_download_dir: Local directory for downloaded files
        state_file: Path to file for tracking download state
        poll_interval: Seconds between checks for new files
        max_concurrent_downloads: Maximum simultaneous downloads
        file_pattern: Pattern to match BUFR files (e.g., "*.BUFR")
        recursive: Whether to search subdirectories recursively
    """

    host: str
    username: str
    password: str
    remote_base_path: str
    local_download_dir: Path
    state_file: Path
    poll_interval: int = 60
    max_concurrent_downloads: int = 5
    file_pattern: str = "*.BUFR"
    recursive: bool = True


class FTPDaemon:
    """
    Daemon service for continuously monitoring and downloading new BUFR files.

    This service runs indefinitely, periodically checking an FTP server for new
    BUFR files that haven't been downloaded yet. It maintains state to avoid
    re-downloading files.

    Example:
        >>> config = FTPDaemonConfig(
        ...     host="ftp.example.com",
        ...     username="user",
        ...     password="pass",
        ...     remote_base_path="/L2",
        ...     local_download_dir=Path("./downloads"),
        ...     state_file=Path("./state.json")
        ... )
        >>> daemon = FTPDaemon(config)
        >>> asyncio.run(daemon.run())  # Runs indefinitely
    """

    def __init__(self, config: FTPDaemonConfig):
        """
        Initialize the FTP daemon.

        Args:
            config: Daemon configuration
        """
        self.config = config
        self.client = FTPClient(config.host, config.username, config.password)
        self.state_tracker = FileStateTracker(config.state_file)
        self._running = False
        self._download_semaphore: Optional[asyncio.Semaphore] = None

        # Ensure local download directory exists
        self.config.local_download_dir.mkdir(parents=True, exist_ok=True)

    async def run(self) -> None:
        """
        Run the daemon indefinitely.

        Continuously monitors the FTP server for new files and downloads them.
        This method runs until interrupted (e.g., Ctrl+C).
        """
        self._running = True
        self._download_semaphore = asyncio.Semaphore(self.config.max_concurrent_downloads)

        logger.info(f"Starting FTP daemon monitoring '{self.config.remote_base_path}'")
        logger.info(f"Downloading to '{self.config.local_download_dir}'")
        logger.info(f"Poll interval: {self.config.poll_interval}s")

        try:
            while self._running:
                try:
                    await self._check_and_download_new_files()
                except Exception as e:
                    logger.error(f"Error during check cycle: {e}", exc_info=True)

                # Wait before next check
                logger.info(f"Waiting {self.config.poll_interval}s before next check...")
                await asyncio.sleep(self.config.poll_interval)

        except asyncio.CancelledError:
            logger.info("Daemon cancelled, shutting down...")
            self._running = False
        except KeyboardInterrupt:
            logger.info("Daemon interrupted, shutting down...")
            self._running = False

    def stop(self) -> None:
        """Stop the daemon gracefully."""
        self._running = False
        logger.info("Daemon stop requested")

    async def _check_and_download_new_files(self) -> None:
        """
        Check for new files and download them.

        This is called once per poll interval.
        """
        logger.info("Checking for new files...")

        # Get list of available files
        new_files = await self._discover_new_files()

        if not new_files:
            logger.info("No new files found")
            return

        logger.info(f"Found {len(new_files)} new files to download")

        # Download files concurrently (with limit)
        tasks = [self._download_file_async(remote_path) for remote_path in new_files]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Log results
        success_count = sum(1 for r in results if r is True)
        error_count = sum(1 for r in results if isinstance(r, Exception))

        logger.info(f"Download cycle complete: {success_count} succeeded, {error_count} failed")

    async def _discover_new_files(self) -> List[str]:
        """
        Discover new files that haven't been downloaded yet.

        Returns:
            List of full remote paths to new files
        """
        new_files = []

        # For synchronous FTP operations, run in executor
        loop = asyncio.get_event_loop()

        try:
            # List files in the remote directory
            files = await loop.run_in_executor(
                None, self.client.list_files, self.config.remote_base_path
            )

            # Filter for BUFR files that haven't been downloaded
            for item in files:
                # Handle both nlst (strings) and mlsd (tuples)
                filename = item if isinstance(item, str) else item[0]

                # Check if it matches the pattern and hasn't been downloaded
                if filename.endswith(".BUFR") and not self.state_tracker.is_downloaded(filename):
                    remote_path = f"{self.config.remote_base_path}/{filename}"
                    new_files.append(remote_path)

        except Exception as e:
            logger.error(f"Failed to discover files: {e}")

        return new_files

    async def _download_file_async(self, remote_path: str) -> bool:
        """
        Download a single file asynchronously.

        Args:
            remote_path: Full remote path to file

        Returns:
            True if successful, False otherwise
        """
        # Use semaphore to limit concurrent downloads
        async with self._download_semaphore:
            filename = Path(remote_path).name
            local_path = self.config.local_download_dir / filename

            try:
                logger.info(f"Downloading {filename}...")

                # Run synchronous FTP operation in executor
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None, self.client.download_file, remote_path, local_path
                )

                # Mark as downloaded
                self.state_tracker.mark_downloaded(filename, remote_path)

                logger.info(f"Successfully downloaded {filename}")
                return True

            except Exception as e:
                logger.error(f"Failed to download {filename}: {e}")
                # Remove from state if it was partially added
                self.state_tracker.remove_file(filename)
                return False

    def get_stats(self) -> dict:
        """
        Get daemon statistics.

        Returns:
            Dictionary with daemon stats
        """
        return {
            "running": self._running,
            "total_downloaded": self.state_tracker.count(),
            "config": {
                "host": self.config.host,
                "remote_path": self.config.remote_base_path,
                "local_dir": str(self.config.local_download_dir),
                "poll_interval": self.config.poll_interval,
            }
        }
