# -*- coding: utf-8 -*-
"""Date-based daemon service for BUFR file monitoring."""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

from .client import FTPClient
from .ftp import parse_ftp_path
from .sqlite_state_tracker import SQLiteStateTracker

logger = logging.getLogger(__name__)


@dataclass
class DateBasedDaemonConfig:
    """
    Configuration for date-based FTP daemon service.

    Attributes:
        host: FTP server hostname or IP
        username: FTP username
        password: FTP password
        remote_base_path: Base path on FTP server (e.g., "/L2")
        radar_code: Radar code to monitor (e.g., "RMA1")
        local_download_dir: Local directory for downloaded files
        state_db: Path to SQLite database for tracking state
        start_date: Start date for scanning (UTC)
        end_date: Optional end date for scanning (UTC). Daemon stops when reached.
        poll_interval: Seconds between checks for new files
        max_concurrent_downloads: Maximum simultaneous downloads
        verify_checksums: Calculate and store file checksums
        resume_partial: Resume partially downloaded files
        volume_types: Optional dict mapping volume codes to valid volume numbers and field types.
                     Format: {'0315': {'01': ['DBZH', 'DBZV'], '02': ['VRAD']}}
                     If None, no filtering is applied.
    """

    host: str
    username: str
    password: str
    remote_base_path: str
    radar_code: str
    local_download_dir: Path
    state_db: Path
    start_date: datetime
    end_date: Optional[datetime] = None
    poll_interval: int = 60
    max_concurrent_downloads: int = 5
    verify_checksums: bool = True
    resume_partial: bool = True
    volume_types: Optional[Dict[str, Dict[str, List[str]]]] = field(default=None)


class DateBasedFTPDaemon:
    """
    Date-based daemon for monitoring and downloading BUFR files.

    Scans FTP directories based on date hierarchy (/L2/RADAR/YYYY/MM/DD/HH/MMSS/)
    and downloads files within specified date range. Supports automatic shutdown
    when end date is reached and all files are retrieved.

    Example:
        >>> from datetime import datetime, timezone
        >>> config = DateBasedDaemonConfig(
        ...     host="ftp.example.com",
        ...     username="user",
        ...     password="pass",
        ...     remote_base_path="/L2",
        ...     radar_code="RMA1",
        ...     local_download_dir=Path("./downloads"),
        ...     state_db=Path("./state.db"),
        ...     start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
        ...     end_date=datetime(2025, 1, 2, tzinfo=timezone.utc)
        ... )
        >>> daemon = DateBasedFTPDaemon(config)
        >>> asyncio.run(daemon.run())
    """

    def __init__(self, config: DateBasedDaemonConfig):
        """
        Initialize the date-based FTP daemon.

        Args:
            config: Daemon configuration
        """
        self.config = config
        self.client = FTPClient(config.host, config.username, config.password)
        self.state_tracker = SQLiteStateTracker(config.state_db)
        self._running = False
        self._download_semaphore: Optional[asyncio.Semaphore] = None
        self._current_scan_date: Optional[datetime] = None

        # Ensure local download directory exists
        self.config.local_download_dir.mkdir(parents=True, exist_ok=True)

    async def run(self) -> None:
        """
        Run the daemon to scan and download files in date range.

        Continuously scans directories based on dates and downloads new files.
        Automatically stops when end_date is reached (if specified) and all
        files in range have been downloaded.
        """
        self._running = True
        self._download_semaphore = asyncio.Semaphore(self.config.max_concurrent_downloads)

        logger.info(f"Starting date-based FTP daemon for radar '{self.config.radar_code}'")
        logger.info(f"Date range: {self.config.start_date} to {self.config.end_date or 'ongoing'}")
        logger.info(f"Downloading to '{self.config.local_download_dir}'")

        # Determine starting point
        self._current_scan_date = self._get_resume_date()

        try:
            while self._running:
                try:
                    # Scan current date
                    files_found = await self._scan_and_download_date(self._current_scan_date)

                    # Move to next date
                    next_date = self._current_scan_date + timedelta(hours=1)

                    # Check if we've reached the end
                    if self.config.end_date and next_date > self.config.end_date:
                        logger.info(f"Reached end date {self.config.end_date}. All files retrieved.")
                        break

                    self._current_scan_date = next_date

                    # If no files found and we're caught up to now, wait
                    if files_found == 0 and next_date > datetime.now(self._current_scan_date.tzinfo):
                        logger.info(f"Caught up to current time. Waiting {self.config.poll_interval}s...")
                        await asyncio.sleep(self.config.poll_interval)

                except Exception as e:
                    logger.error(f"Error during scan cycle: {e}", exc_info=True)
                    await asyncio.sleep(self.config.poll_interval)

        except asyncio.CancelledError:
            logger.info("Daemon cancelled, shutting down...")
        except KeyboardInterrupt:
            logger.info("Daemon interrupted, shutting down...")
        finally:
            self._running = False
            self.state_tracker.close()

    def stop(self) -> None:
        """Stop the daemon gracefully."""
        self._running = False
        logger.info("Daemon stop requested")

    def _get_resume_date(self) -> datetime:
        """
        Determine the date to resume scanning from.

        Returns:
            datetime to start/resume scanning
        """
        # Check if we have any downloaded files for this radar
        files = self.state_tracker.get_files_by_date_range(
            self.config.start_date,
            self.config.end_date or datetime.now(self.config.start_date.tzinfo),
            radar_code=self.config.radar_code,
        )

        if files:
            # Get the latest file info
            latest_file = files[-1]
            file_info = self.state_tracker.get_file_info(latest_file)
            if file_info and file_info["observation_datetime"]:
                resume_dt = datetime.fromisoformat(file_info["observation_datetime"])
                logger.info(f"Resuming from last downloaded file at {resume_dt}")
                return resume_dt

        logger.info(f"Starting from beginning: {self.config.start_date}")
        return self.config.start_date

    async def _scan_and_download_date(self, scan_date: datetime) -> int:
        """
        Scan a specific date/hour and download new files.

        Args:
            scan_date: Date/hour to scan

        Returns:
            Number of new files found
        """
        # Build remote path for this date
        # Format: /L2/RMA1/YYYY/MM/DD/HH/
        remote_path = (
            f"{self.config.remote_base_path}/{self.config.radar_code}/"
            f"{scan_date.year:04d}/{scan_date.month:02d}/{scan_date.day:02d}/"
            f"{scan_date.hour:02d}"
        )

        logger.debug(f"Scanning {remote_path}")

        try:
            # List directories (MMSS format)
            loop = asyncio.get_event_loop()
            minute_dirs = await loop.run_in_executor(None, self._list_minute_directories, remote_path)

            new_files_count = 0

            for minute_dir in minute_dirs:
                files_dir = f"{remote_path}/{minute_dir}"
                files = await loop.run_in_executor(None, self._list_bufr_files, files_dir)

                for filename in files:
                    if not self.state_tracker.is_downloaded(filename):
                        remote_file_path = f"{files_dir}/{filename}"
                        await self._download_file_async(remote_file_path, filename)
                        new_files_count += 1

            if new_files_count > 0:
                logger.info(f"Downloaded {new_files_count} new files from {remote_path}")

            return new_files_count

        except Exception as e:
            logger.debug(f"Could not scan {remote_path}: {e}")
            return 0

    def _list_minute_directories(self, remote_path: str) -> List[str]:
        """
        List minute/second directories (MMSS format).

        Args:
            remote_path: Path to hour directory

        Returns:
            List of MMSS directory names
        """
        try:
            items = self.client.list_files(remote_path)
            # Filter for 4-digit directories (MMSS format)
            return [item for item in items if isinstance(item, str) and item.isdigit() and len(item) == 4]
        except Exception as e:
            logger.debug(f"Could not list directories in {remote_path}: {e}")
            return []

    def _list_bufr_files(self, remote_path: str) -> List[str]:
        """
        List BUFR files in a directory.

        Args:
            remote_path: Path to MMSS directory

        Returns:
            List of BUFR filenames
        """
        try:
            items = self.client.list_files(remote_path)
            bufr_files = [item for item in items if isinstance(item, str) and item.endswith(".BUFR")]
            return self._filter_files_by_volume(bufr_files)
        except Exception as e:
            logger.debug(f"Could not list files in {remote_path}: {e}")
            return []

    def _filter_files_by_volume(self, filenames: List[str]) -> List[str]:
        """
        Filter files based on volume types configuration.

        Args:
            filenames: List of BUFR filenames

        Returns:
            Filtered list of filenames based on volume_types config
        """
        if not self.config.volume_types:
            # No filtering if volume_types not configured
            return filenames

        filtered = []
        for filename in filenames:
            try:
                # Parse filename: RMA1_0315_03_DBZH_20250925T000534Z.BUFR
                parts = filename.split("_")
                if len(parts) < 4:
                    logger.debug(f"Skipping file with unexpected format: {filename}")
                    continue

                vol_code = parts[1]  # e.g., "0315"
                vol_number = parts[2]  # e.g., "03"
                field_type = parts[3]  # e.g., "DBZH"

                # Check if volume code is in configuration
                if vol_code not in self.config.volume_types:
                    logger.debug(f"Skipping {filename}: volume code {vol_code} not in configured types")
                    continue

                # Check if volume number is valid for this volume code
                vol_config = self.config.volume_types[vol_code]
                if vol_number not in vol_config:
                    logger.debug(
                        f"Skipping {filename}: volume number {vol_number} not valid for volume code {vol_code}"
                    )
                    continue

                # Check if field type is valid for this volume number
                valid_fields = vol_config[vol_number]
                if field_type not in valid_fields:
                    logger.debug(
                        f"Skipping {filename}: field type {field_type} not valid for vol {vol_code}/{vol_number}"
                    )
                    continue

                # File passes all filters
                filtered.append(filename)
                logger.debug(f"Accepted {filename}: vol {vol_code}/{vol_number}, field {field_type}")

            except Exception as e:
                logger.warning(f"Error parsing filename {filename}: {e}")
                continue

        return filtered

    async def _download_file_async(self, remote_path: str, filename: str) -> bool:
        """
        Download a single file asynchronously with checksum verification.

        Args:
            remote_path: Full remote path to file
            filename: Name of the file

        Returns:
            True if successful, False otherwise
        """
        async with self._download_semaphore:
            local_path = self.config.local_download_dir / filename

            try:
                logger.info(f"Downloading {filename}...")

                # Download file
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, self.client.download_file, remote_path, local_path)

                # Calculate checksum if enabled
                checksum = None
                if self.config.verify_checksums:
                    checksum = await loop.run_in_executor(None, SQLiteStateTracker.calculate_checksum, local_path)

                # Get file size
                file_size = local_path.stat().st_size

                # Parse metadata from path
                try:
                    metadata = parse_ftp_path(remote_path)
                    metadata["observation_datetime"] = metadata["datetime"].isoformat()
                except Exception:
                    metadata = {}

                # Mark as downloaded
                self.state_tracker.mark_downloaded(
                    filename,
                    remote_path,
                    str(local_path),
                    file_size,
                    checksum,
                    metadata,
                )

                logger.info(f"Successfully downloaded {filename} ({file_size} bytes)")
                return True

            except Exception as e:
                logger.error(f"Failed to download {filename}: {e}")

                # Mark as partial if file exists
                if local_path.exists():
                    bytes_downloaded = local_path.stat().st_size
                    self.state_tracker.mark_partial_download(filename, remote_path, str(local_path), bytes_downloaded)

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
            "current_scan_date": self._current_scan_date.isoformat() if self._current_scan_date else None,
            "config": {
                "host": self.config.host,
                "radar_code": self.config.radar_code,
                "start_date": self.config.start_date.isoformat(),
                "end_date": self.config.end_date.isoformat() if self.config.end_date else None,
                "local_dir": str(self.config.local_download_dir),
            },
        }
