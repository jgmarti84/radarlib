# -*- coding: utf-8 -*-
"""SQLite-based state tracking for downloaded BUFR files."""

import hashlib
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class SQLiteStateTracker:
    """
    Track downloaded BUFR files using SQLite database.

    Provides better performance and query capabilities compared to JSON,
    especially for large numbers of files. Tracks download progress,
    checksums, and file metadata.

    Example:
        >>> tracker = SQLiteStateTracker("./download_state.db")
        >>> tracker.mark_downloaded("file.BUFR", "/L2/RMA1/2025/01/01/18/3020/file.BUFR")
        >>> if not tracker.is_downloaded("file2.BUFR"):
        ...     # Download file2.BUFR
        ...     tracker.mark_downloaded("file2.BUFR", "/L2/RMA1/2025/01/01/18/3025/file2.BUFR")
    """

    def __init__(self, db_path: Path):
        """
        Initialize the SQLite state tracker.

        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: Optional[sqlite3.Connection] = None
        self._init_database()

    def _init_database(self) -> None:
        """Initialize database schema."""
        conn = self._get_connection()
        cursor = conn.cursor()

        # Main downloads table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS downloads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT UNIQUE NOT NULL,
                remote_path TEXT NOT NULL,
                local_path TEXT,
                downloaded_at TEXT NOT NULL,
                file_size INTEGER,
                checksum TEXT,
                radar_code TEXT,
                field_type TEXT,
                observation_datetime TEXT,
                status TEXT DEFAULT 'completed',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """
        )

        # Index for faster queries
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_filename ON downloads(filename)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_radar_datetime ON downloads(radar_code, observation_datetime)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_status ON downloads(status)")

        # Partial downloads table for resuming
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS partial_downloads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT UNIQUE NOT NULL,
                remote_path TEXT NOT NULL,
                local_path TEXT NOT NULL,
                bytes_downloaded INTEGER DEFAULT 0,
                total_bytes INTEGER,
                partial_checksum TEXT,
                last_attempt TEXT NOT NULL,
                attempt_count INTEGER DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """
        )

        conn.commit()
        logger.info(f"Initialized SQLite database at {self.db_path}")

    def _get_connection(self) -> sqlite3.Connection:
        """Get database connection."""
        if self._conn is None:
            self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def close(self) -> None:
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def is_downloaded(self, filename: str) -> bool:
        """
        Check if a file has been successfully downloaded.

        Args:
            filename: Name of the file to check

        Returns:
            True if file has been downloaded, False otherwise
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT 1 FROM downloads WHERE filename = ? AND status = 'completed'",
            (filename,),
        )
        return cursor.fetchone() is not None

    def mark_downloaded(
        self,
        filename: str,
        remote_path: str,
        local_path: Optional[str] = None,
        file_size: Optional[int] = None,
        checksum: Optional[str] = None,
        metadata: Optional[Dict] = None,
    ) -> None:
        """
        Mark a file as successfully downloaded.

        Args:
            filename: Name of the downloaded file
            remote_path: Full remote path where file was located
            local_path: Local path where file was saved
            file_size: Size of the file in bytes
            checksum: SHA256 checksum of the file
            metadata: Optional metadata (radar, field, timestamp, etc.)
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        now = datetime.now(timezone.utc).isoformat()

        metadata = metadata or {}

        cursor.execute(
            """
            INSERT OR REPLACE INTO downloads
            (filename, remote_path, local_path, downloaded_at, file_size, checksum,
             radar_code, field_type, observation_datetime, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'completed', ?, ?)
        """,
            (
                filename,
                remote_path,
                local_path,
                now,
                file_size,
                checksum,
                metadata.get("radar_code"),
                metadata.get("field_type"),
                metadata.get("observation_datetime"),
                now,
                now,
            ),
        )

        # Remove from partial downloads if exists
        cursor.execute("DELETE FROM partial_downloads WHERE filename = ?", (filename,))

        conn.commit()
        logger.debug(f"Marked '{filename}' as downloaded")

    def mark_partial_download(
        self,
        filename: str,
        remote_path: str,
        local_path: str,
        bytes_downloaded: int,
        total_bytes: Optional[int] = None,
        partial_checksum: Optional[str] = None,
    ) -> None:
        """
        Mark a file as partially downloaded for later resumption.

        Args:
            filename: Name of the file
            remote_path: Full remote path
            local_path: Local path where partial file is saved
            bytes_downloaded: Number of bytes already downloaded
            total_bytes: Total file size if known
            partial_checksum: Checksum of downloaded portion
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        now = datetime.now(timezone.utc).isoformat()

        cursor.execute(
            "SELECT attempt_count FROM partial_downloads WHERE filename = ?",
            (filename,),
        )
        row = cursor.fetchone()
        attempt_count = (row[0] + 1) if row else 1

        cursor.execute(
            """
            INSERT OR REPLACE INTO partial_downloads
            (filename, remote_path, local_path, bytes_downloaded, total_bytes,
             partial_checksum, last_attempt, attempt_count, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?,
                    COALESCE((SELECT created_at FROM partial_downloads WHERE filename = ?), ?), ?)
        """,
            (
                filename,
                remote_path,
                local_path,
                bytes_downloaded,
                total_bytes,
                partial_checksum,
                now,
                attempt_count,
                filename,
                now,
                now,
            ),
        )

        conn.commit()
        logger.debug(f"Marked '{filename}' as partial ({bytes_downloaded} bytes, attempt {attempt_count})")

    def get_partial_download(self, filename: str) -> Optional[Dict]:
        """
        Get information about a partial download.

        Args:
            filename: Name of the file

        Returns:
            Dictionary with partial download info, or None if not found
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM partial_downloads WHERE filename = ?", (filename,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def get_downloaded_files(self) -> Set[str]:
        """
        Get set of all successfully downloaded filenames.

        Returns:
            Set of filenames that have been downloaded
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT filename FROM downloads WHERE status = 'completed'")
        return {row[0] for row in cursor.fetchall()}

    def get_file_info(self, filename: str) -> Optional[Dict]:
        """
        Get information about a downloaded file.

        Args:
            filename: Name of the file

        Returns:
            Dictionary with download info, or None if not found
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM downloads WHERE filename = ?", (filename,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def get_files_by_date_range(
        self, start_date: datetime, end_date: datetime, radar_code: Optional[str] = None
    ) -> List[str]:
        """
        Get files downloaded within a date range.

        Args:
            start_date: Start of date range
            end_date: End of date range
            radar_code: Optional radar code to filter by

        Returns:
            List of filenames in the range
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        start_iso = start_date.isoformat()
        end_iso = end_date.isoformat()

        if radar_code:
            cursor.execute(
                """
                SELECT filename FROM downloads
                WHERE observation_datetime >= ? AND observation_datetime <= ?
                AND radar_code = ? AND status = 'completed'
                ORDER BY observation_datetime
            """,
                (start_iso, end_iso, radar_code),
            )
        else:
            cursor.execute(
                """
                SELECT filename FROM downloads
                WHERE observation_datetime >= ? AND observation_datetime <= ?
                AND status = 'completed'
                ORDER BY observation_datetime
            """,
                (start_iso, end_iso),
            )

        return [row[0] for row in cursor.fetchall()]

    def count(self, status: str = "completed") -> int:
        """
        Get total number of downloaded files.

        Args:
            status: Filter by status (default: 'completed')

        Returns:
            Count of files
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM downloads WHERE status = ?", (status,))
        return cursor.fetchone()[0]

    def remove_file(self, filename: str) -> None:
        """
        Remove a file from the state.

        Args:
            filename: Name of the file to remove
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM downloads WHERE filename = ?", (filename,))
        cursor.execute("DELETE FROM partial_downloads WHERE filename = ?", (filename,))
        conn.commit()
        logger.debug(f"Removed '{filename}' from state")

    def clear(self, include_partials: bool = True) -> None:
        """
        Clear all state.

        Args:
            include_partials: Also clear partial downloads
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM downloads")
        if include_partials:
            cursor.execute("DELETE FROM partial_downloads")
        conn.commit()
        logger.info("Cleared all state")

    @staticmethod
    def calculate_checksum(file_path: Path) -> str:
        """
        Calculate SHA256 checksum of a file.

        Args:
            file_path: Path to file

        Returns:
            Hexadecimal checksum string
        """
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        return sha256.hexdigest()
