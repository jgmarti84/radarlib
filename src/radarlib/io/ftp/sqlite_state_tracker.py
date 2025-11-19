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

        # Volume processing table for tracking processed volumes
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS volume_processing (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                volume_id TEXT UNIQUE NOT NULL,
                radar_code TEXT NOT NULL,
                vol_code TEXT NOT NULL,
                vol_number TEXT NOT NULL,
                observation_datetime TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                netcdf_path TEXT,
                processed_at TEXT,
                error_message TEXT,
                is_complete INTEGER DEFAULT 0,
                expected_fields TEXT,
                downloaded_fields TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """
        )

        # Index for faster queries on volume processing
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_volume_id ON volume_processing(volume_id)")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_volume_radar_datetime ON "
            "volume_processing(radar_code, observation_datetime)"
        )
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_volume_status ON volume_processing(status)")

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

    # Volume processing methods

    def get_volume_id(self, radar_code: str, vol_code: str, vol_number: str, observation_datetime: str) -> str:
        """
        Generate a unique volume ID.

        Args:
            radar_code: Radar code (e.g., "RMA1")
            vol_code: Volume code (e.g., "0315")
            vol_number: Volume number (e.g., "01")
            observation_datetime: ISO format datetime string

        Returns:
            Unique volume ID string
        """
        return f"{radar_code}_{vol_code}_{vol_number}_{observation_datetime}"

    def register_volume(
        self,
        volume_id: str,
        radar_code: str,
        vol_code: str,
        vol_number: str,
        observation_datetime: str,
        expected_fields: List[str],
        is_complete: bool = False,
    ) -> None:
        """
        Register a new volume for processing.

        Args:
            volume_id: Unique volume identifier
            radar_code: Radar code
            vol_code: Volume code
            vol_number: Volume number
            observation_datetime: ISO format datetime
            expected_fields: List of expected field types
            is_complete: Whether all fields are downloaded
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        now = datetime.now(timezone.utc).isoformat()

        cursor.execute(
            """
            INSERT OR REPLACE INTO volume_processing
            (volume_id, radar_code, vol_code, vol_number, observation_datetime,
             expected_fields, downloaded_fields, is_complete, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, '', ?, 'pending', ?, ?)
        """,
            (
                volume_id,
                radar_code,
                vol_code,
                vol_number,
                observation_datetime,
                ",".join(expected_fields),
                1 if is_complete else 0,
                now,
                now,
            ),
        )
        conn.commit()
        logger.debug(f"Registered volume '{volume_id}' with status complete={is_complete}")

    def update_volume_fields(self, volume_id: str, downloaded_fields: List[str], is_complete: bool) -> None:
        """
        Update the list of downloaded fields for a volume.

        Args:
            volume_id: Unique volume identifier
            downloaded_fields: List of downloaded field types
            is_complete: Whether all expected fields are now downloaded
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        now = datetime.now(timezone.utc).isoformat()

        cursor.execute(
            """
            UPDATE volume_processing
            SET downloaded_fields = ?, is_complete = ?, updated_at = ?
            WHERE volume_id = ?
        """,
            (",".join(downloaded_fields), 1 if is_complete else 0, now, volume_id),
        )
        conn.commit()
        logger.debug(f"Updated volume '{volume_id}' fields: {downloaded_fields}, complete={is_complete}")

    def mark_volume_processing(
        self, volume_id: str, status: str, netcdf_path: Optional[str] = None, error_message: Optional[str] = None
    ) -> None:
        """
        Mark a volume as being processed or completed.

        Args:
            volume_id: Unique volume identifier
            status: Processing status ('processing', 'completed', 'failed')
            netcdf_path: Path to generated NetCDF file (if completed)
            error_message: Error message (if failed)
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        now = datetime.now(timezone.utc).isoformat()

        processed_at = now if status == "completed" else None

        cursor.execute(
            """
            UPDATE volume_processing
            SET status = ?, netcdf_path = ?, processed_at = ?, error_message = ?, updated_at = ?
            WHERE volume_id = ?
        """,
            (status, netcdf_path, processed_at, error_message, now, volume_id),
        )
        conn.commit()
        logger.debug(f"Marked volume '{volume_id}' as {status}")

    def get_volumes_by_status(self, status: str = "pending", limit: Optional[int] = None) -> List[Dict]:
        """
        Get volumes by their processing status.

        Args:
            status: Status to filter by ('pending', 'processing', 'completed', 'failed')
            limit: Optional limit on number of results

        Returns:
            List of volume dictionaries
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        query = "SELECT * FROM volume_processing WHERE status = ? ORDER BY observation_datetime"
        if limit:
            query += f" LIMIT {limit}"

        cursor.execute(query, (status,))
        return [dict(row) for row in cursor.fetchall()]

    def get_complete_unprocessed_volumes(self, limit: Optional[int] = None) -> List[Dict]:
        """
        Get complete volumes that haven't been processed yet.

        Args:
            limit: Optional limit on number of results

        Returns:
            List of volume dictionaries
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        query = """
            SELECT * FROM volume_processing
            WHERE is_complete = 1 AND status = 'pending'
            ORDER BY observation_datetime
        """
        if limit:
            query += f" LIMIT {limit}"

        cursor.execute(query)
        return [dict(row) for row in cursor.fetchall()]

    def get_volume_info(self, volume_id: str) -> Optional[Dict]:
        """
        Get information about a specific volume.

        Args:
            volume_id: Unique volume identifier

        Returns:
            Dictionary with volume info, or None if not found
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM volume_processing WHERE volume_id = ?", (volume_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def get_volume_files(self, radar_code: str, vol_code: str, vol_number: str, observation_datetime: str) -> List[str]:
        """
        Get all downloaded files for a specific volume.

        Args:
            radar_code: Radar code
            vol_code: Volume code
            vol_number: Volume number
            observation_datetime: ISO format datetime

        Returns:
            List of filenames belonging to this volume
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        # Files for a volume have same radar, vol_code, vol_number, and observation_datetime
        # We need to parse filename to extract these components
        cursor.execute(
            """
            SELECT filename FROM downloads
            WHERE radar_code = ? AND observation_datetime = ? AND status = 'completed'
        """,
            (radar_code, observation_datetime),
        )

        files = []
        for row in cursor.fetchall():
            filename = row[0]
            # Parse filename: RADAR_VOLCODE_VOLNUM_FIELD_TIMESTAMP.BUFR
            parts = filename.split("_")
            if len(parts) >= 3 and parts[1] == vol_code and parts[2] == vol_number:
                files.append(filename)

        return files
