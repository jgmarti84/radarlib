"""
FTP daemon service for monitoring and fetching radar BUFR files.

This module provides a daemon service that continuously monitors an FTP server
for new BUFR files and downloads them automatically.
"""

import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, List, Optional, Set

from radarlib.io.ftp.client import AsyncFTPClient


@dataclass
class FTPDaemonConfig:
    """
    Configuration for the FTP daemon service.

    Attributes:
        host: FTP server hostname or IP address
        username: FTP username
        password: FTP password
        port: FTP port (default: 21)
        remote_path: Remote directory to monitor
        local_dir: Local directory to save downloaded files
        file_pattern: File pattern to match (default: '*.BUFR')
        poll_interval: Seconds between checks for new files (default: 60)
        max_concurrent_downloads: Maximum concurrent downloads (default: 5)
        recursive: Whether to search recursively (default: False)
    """

    host: str
    username: str
    password: str
    remote_path: str
    local_dir: str
    port: int = 21
    file_pattern: str = "*.BUFR"
    poll_interval: int = 60
    max_concurrent_downloads: int = 5
    recursive: bool = False


class FTPDaemon:
    """
    Async daemon service for monitoring FTP server for new BUFR files.

    This daemon continuously monitors an FTP server directory and downloads
    new files as they appear. It maintains a set of already-processed files
    to avoid re-downloading.

    Example:
        >>> config = FTPDaemonConfig(
        ...     host='ftp.example.com',
        ...     username='user',
        ...     password='pass',
        ...     remote_path='/radar/data',
        ...     local_dir='/local/data'
        ... )
        >>> daemon = FTPDaemon(config)
        >>> await daemon.run()
    """

    def __init__(
        self,
        config: FTPDaemonConfig,
        on_file_downloaded: Optional[Callable[[str], None]] = None,
        logger: Optional[logging.Logger] = None,
    ):
        """
        Initialize the FTP daemon.

        Args:
            config: Daemon configuration
            on_file_downloaded: Optional callback function called when a file is downloaded.
                               Receives the local file path as argument.
            logger: Optional logger instance
        """
        self.config = config
        self.on_file_downloaded = on_file_downloaded
        self.logger = logger or logging.getLogger(__name__)
        self._processed_files: Set[str] = set()
        self._running = False
        self._client: Optional[AsyncFTPClient] = None

    async def initialize(self):
        """
        Initialize the daemon by scanning for existing files.

        This prevents re-downloading files that already exist locally.
        """
        self.logger.info("Initializing FTP daemon...")
        local_dir = Path(self.config.local_dir)

        if local_dir.exists():
            # Get list of already downloaded files
            existing_files = list(local_dir.glob(self.config.file_pattern))
            self._processed_files = {f.name for f in existing_files}
            self.logger.info(f"Found {len(self._processed_files)} existing files in {self.config.local_dir}")
        else:
            self.logger.info(f"Local directory {self.config.local_dir} does not exist, will be created")

        self.logger.info("Daemon initialized")

    async def _check_and_download_new_files(self):
        """
        Check for new files on FTP server and download them.

        Returns:
            Number of new files downloaded
        """
        try:
            # Connect to FTP server
            self._client = AsyncFTPClient(
                host=self.config.host,
                username=self.config.username,
                password=self.config.password,
                port=self.config.port,
                logger=self.logger,
            )

            await self._client.connect()

            try:
                # List files on FTP server
                remote_files = await self._client.list_files(
                    self.config.remote_path, pattern=self.config.file_pattern, recursive=self.config.recursive
                )

                # Filter out already processed files
                new_files = []
                for remote_path in remote_files:
                    filename = Path(remote_path).name
                    if filename not in self._processed_files:
                        new_files.append(remote_path)

                if new_files:
                    self.logger.info(f"Found {len(new_files)} new files to download")

                    # Download new files
                    downloaded = await self._client.download_files(
                        new_files, self.config.local_dir, max_concurrent=self.config.max_concurrent_downloads
                    )

                    # Update processed files set
                    for local_path in downloaded:
                        filename = Path(local_path).name
                        self._processed_files.add(filename)

                        # Call callback if provided
                        if self.on_file_downloaded:
                            try:
                                self.on_file_downloaded(local_path)
                            except Exception as e:
                                self.logger.error(f"Error in file downloaded callback: {e}")

                    return len(downloaded)
                else:
                    self.logger.debug("No new files found")
                    return 0

            finally:
                await self._client.disconnect()
                self._client = None

        except Exception as e:
            self.logger.error(f"Error checking for new files: {e}", exc_info=True)
            return 0

    async def run(self, max_iterations: Optional[int] = None):
        """
        Run the daemon service.

        This starts the monitoring loop that continuously checks for new files.

        Args:
            max_iterations: Optional maximum number of iterations (for testing).
                          If None, runs indefinitely.
        """
        self.logger.info("Starting FTP daemon service")
        self.logger.info(f"  Host: {self.config.host}:{self.config.port}")
        self.logger.info(f"  Remote path: {self.config.remote_path}")
        self.logger.info(f"  Local directory: {self.config.local_dir}")
        self.logger.info(f"  Poll interval: {self.config.poll_interval}s")

        self._running = True
        iteration = 0

        try:
            # Initialize daemon
            await self.initialize()

            # Main monitoring loop
            while self._running:
                self.logger.info(f"Checking for new files (iteration {iteration + 1})...")

                # Check and download new files
                downloaded_count = await self._check_and_download_new_files()

                if downloaded_count > 0:
                    self.logger.info(f"Downloaded {downloaded_count} new files")

                # Check if we should stop (for testing)
                iteration += 1
                if max_iterations is not None and iteration >= max_iterations:
                    self.logger.info(f"Reached max iterations ({max_iterations}), stopping daemon")
                    break

                # Wait before next check
                if self._running:
                    self.logger.debug(f"Waiting {self.config.poll_interval}s before next check")
                    await asyncio.sleep(self.config.poll_interval)

        except asyncio.CancelledError:
            self.logger.info("Daemon service cancelled")
            raise
        except Exception as e:
            self.logger.error(f"Fatal error in daemon service: {e}", exc_info=True)
            raise
        finally:
            self._running = False
            if self._client:
                await self._client.disconnect()
            self.logger.info("Daemon service stopped")

    def stop(self):
        """Stop the daemon service."""
        self.logger.info("Stopping daemon service...")
        self._running = False

    async def run_once(self):
        """
        Run a single check for new files (useful for testing or one-off downloads).

        Returns:
            Number of files downloaded
        """
        await self.initialize()
        return await self._check_and_download_new_files()

    def get_processed_files(self) -> List[str]:
        """
        Get list of files that have been processed.

        Returns:
            List of processed filenames
        """
        return sorted(list(self._processed_files))
