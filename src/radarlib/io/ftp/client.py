"""
Async FTP client for downloading radar BUFR files.

This module provides an async wrapper around aioftp for fetching files from FTP servers.
"""

import asyncio
import logging
from pathlib import Path
from typing import List, Optional

import aioftp


class AsyncFTPClient:
    """
    Async FTP client for downloading radar data files.

    This client provides async operations for connecting to FTP servers,
    listing files, and downloading BUFR radar data files.

    Example:
        >>> async with AsyncFTPClient('ftp.example.com', 'user', 'pass') as client:
        ...     files = await client.list_files('/radar/data')
        ...     await client.download_file('/radar/data/file.BUFR', '/local/path/')
    """

    def __init__(
        self,
        host: str,
        username: str = "anonymous",
        password: str = "",
        port: int = 21,
        logger: Optional[logging.Logger] = None,
    ):
        """
        Initialize FTP client.

        Args:
            host: FTP server hostname or IP address
            username: FTP username (default: 'anonymous')
            password: FTP password (default: empty string)
            port: FTP port (default: 21)
            logger: Optional logger instance
        """
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.logger = logger or logging.getLogger(__name__)
        self._client: Optional[aioftp.Client] = None

    async def __aenter__(self):
        """Context manager entry - connect to FTP server."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - disconnect from FTP server."""
        await self.disconnect()

    async def connect(self):
        """
        Connect to the FTP server.

        Raises:
            aioftp.errors.StatusCodeError: If connection fails
        """
        self.logger.info(f"Connecting to FTP server {self.host}:{self.port}")
        self._client = aioftp.Client()
        await self._client.connect(self.host, self.port)
        await self._client.login(self.username, self.password)
        self.logger.info("Successfully connected to FTP server")

    async def disconnect(self):
        """Disconnect from the FTP server."""
        if self._client:
            self.logger.info("Disconnecting from FTP server")
            await self._client.quit()
            self._client = None

    async def list_files(
        self, remote_path: str, pattern: str = "*.BUFR", recursive: bool = False
    ) -> List[str]:
        """
        List files in a remote directory.

        Args:
            remote_path: Remote directory path
            pattern: File pattern to match (default: '*.BUFR')
            recursive: Whether to search recursively (default: False)

        Returns:
            List of file paths matching the pattern

        Raises:
            ValueError: If not connected to server
        """
        if not self._client:
            raise ValueError("Not connected to FTP server. Call connect() first.")

        self.logger.info(f"Listing files in {remote_path} with pattern {pattern}")

        files = []
        try:
            async for path, info in self._client.list(remote_path, recursive=recursive):
                if info["type"] == "file":
                    file_path = str(path)
                    # Simple pattern matching for BUFR files
                    if pattern == "*.BUFR" and file_path.endswith(".BUFR"):
                        files.append(file_path)
                    elif pattern in file_path:
                        files.append(file_path)

            self.logger.info(f"Found {len(files)} files matching pattern")
            return files

        except Exception as e:
            self.logger.error(f"Error listing files: {e}")
            raise

    async def download_file(self, remote_path: str, local_dir: str) -> str:
        """
        Download a single file from the FTP server.

        Args:
            remote_path: Remote file path
            local_dir: Local directory to save the file

        Returns:
            Path to the downloaded file

        Raises:
            ValueError: If not connected to server
        """
        if not self._client:
            raise ValueError("Not connected to FTP server. Call connect() first.")

        # Ensure local directory exists
        local_dir_path = Path(local_dir)
        local_dir_path.mkdir(parents=True, exist_ok=True)

        # Extract filename from remote path
        filename = Path(remote_path).name
        local_path = local_dir_path / filename

        self.logger.info(f"Downloading {remote_path} to {local_path}")

        try:
            await self._client.download(remote_path, local_path, write_into=True)
            self.logger.info(f"Successfully downloaded {filename}")
            return str(local_path)

        except Exception as e:
            self.logger.error(f"Error downloading {remote_path}: {e}")
            raise

    async def download_files(
        self, remote_files: List[str], local_dir: str, max_concurrent: int = 5
    ) -> List[str]:
        """
        Download multiple files concurrently from the FTP server.

        Args:
            remote_files: List of remote file paths
            local_dir: Local directory to save files
            max_concurrent: Maximum number of concurrent downloads (default: 5)

        Returns:
            List of local file paths for successfully downloaded files
        """
        self.logger.info(f"Downloading {len(remote_files)} files with max_concurrent={max_concurrent}")

        # Use a semaphore to limit concurrent downloads
        semaphore = asyncio.Semaphore(max_concurrent)

        async def download_with_semaphore(remote_path: str) -> Optional[str]:
            async with semaphore:
                try:
                    return await self.download_file(remote_path, local_dir)
                except Exception as e:
                    self.logger.error(f"Failed to download {remote_path}: {e}")
                    return None

        # Download all files concurrently (up to max_concurrent at a time)
        results = await asyncio.gather(*[download_with_semaphore(f) for f in remote_files])

        # Filter out None values (failed downloads)
        downloaded_files = [f for f in results if f is not None]
        self.logger.info(f"Successfully downloaded {len(downloaded_files)}/{len(remote_files)} files")

        return downloaded_files

    async def check_file_exists(self, remote_path: str) -> bool:
        """
        Check if a file exists on the FTP server.

        Args:
            remote_path: Remote file path to check

        Returns:
            True if file exists, False otherwise

        Raises:
            ValueError: If not connected to server
        """
        if not self._client:
            raise ValueError("Not connected to FTP server. Call connect() first.")

        try:
            await self._client.stat(remote_path)
            return True
        except aioftp.errors.StatusCodeError:
            return False
