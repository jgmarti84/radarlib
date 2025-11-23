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
                radar_name TEXT,
                strategy TEXT,
                vol_nr TEXT,
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
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_radar_datetime ON downloads(radar_name, observation_datetime)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_status ON downloads(status)")

        # Volume processing table for tracking processed volumes
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS volume_processing (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                volume_id TEXT UNIQUE NOT NULL,
                radar_name TEXT NOT NULL,
                strategy TEXT NOT NULL,
                vol_nr TEXT NOT NULL,
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
            "volume_processing(radar_name, observation_datetime)"
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
        radar_name: Optional[str] = None,
        strategy: Optional[str] = None,
        vol_nr: Optional[str] = None,
        field_type: Optional[str] = None,
        observation_datetime: Optional[str] = None,
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

        cursor.execute(
            """
            INSERT OR REPLACE INTO downloads
            (filename, remote_path, local_path, downloaded_at, file_size, checksum,
             radar_name, strategy, vol_nr, field_type, observation_datetime, status,
             created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'completed', ?, ?)
        """,
            (
                filename,
                remote_path,
                local_path,
                now,
                file_size,
                checksum,
                radar_name,
                strategy,
                vol_nr,
                field_type,
                observation_datetime,
                now,
                now,
            ),
        )

        conn.commit()
        logger.debug(f"Marked '{filename}' as downloaded")

    def mark_failed(
        self,
        filename: str,
        remote_path: str,
        local_path: Optional[str] = None,
        file_size: Optional[int] = None,
        checksum: Optional[str] = None,
        radar_name: Optional[str] = None,
        strategy: Optional[str] = None,
        vol_nr: Optional[str] = None,
        field_type: Optional[str] = None,
        observation_datetime: Optional[str] = None,
    ) -> None:
        """
        Mark a file as failed to download.

        Args:
            filename: Name of the failed file
            remote_path: Full remote path where file was located
            local_path: Local path where file was saved
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        now = datetime.now(timezone.utc).isoformat()

        cursor.execute(
            """
            INSERT OR REPLACE INTO downloads
            (filename, remote_path, local_path, downloaded_at, file_size, checksum,
             radar_name, strategy, vol_nr, field_type, observation_datetime, status,
             created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'failed', ?, ?)
        """,
            (
                filename,
                remote_path,
                local_path,
                now,
                file_size,
                checksum,
                radar_name,
                strategy,
                vol_nr,
                field_type,
                observation_datetime,
                now,
                now,
            ),
        )

        conn.commit()
        logger.debug(f"Marked '{filename}' as failed")

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
        self, start_date: datetime, end_date: datetime, radar_name: Optional[str] = None
    ) -> List[str]:
        """
        Get files downloaded within a date range.

        Args:
            start_date: Start of date range
            end_date: End of date range
            radar_name: Optional radar name to filter by

        Returns:
            List of filenames in the range
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        start_iso = start_date.isoformat()
        end_iso = end_date.isoformat()

        if radar_name:
            cursor.execute(
                """
                SELECT filename FROM downloads
                WHERE observation_datetime >= ? AND observation_datetime <= ?
                AND radar_name = ? AND status = 'completed'
                ORDER BY observation_datetime
            """,
                (start_iso, end_iso, radar_name),
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
        # cursor.execute("DELETE FROM partial_downloads WHERE filename = ?", (filename,))
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

    def get_latest_downloaded_file(self, radar_name: Optional[str] = None) -> Optional[Dict]:
        """
        Get the downloaded BUFR file with the latest observation time.

        Args:
            radar_name: Optional filter by radar name. If None, returns latest across all radars.

        Returns:
            Dictionary with file info (filename, remote_path, local_path, observation_datetime, etc.)
            or None if no files found.
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        if radar_name:
            cursor.execute(
                """
                SELECT * FROM downloads
                WHERE status = 'completed' AND radar_name = ?
                ORDER BY observation_datetime DESC
                LIMIT 1
            """,
                (radar_name,),
            )
        else:
            cursor.execute(
                """
                SELECT * FROM downloads
                WHERE status = 'completed'
                ORDER BY observation_datetime DESC
                LIMIT 1
            """
            )

        row = cursor.fetchone()
        return dict(row) if row else None

    # Volume processing methods

    def get_volume_id(self, radar_name: str, strategy: str, vol_nr: str, observation_datetime: str) -> str:
        """
        Generate unique volume identifier.

        Args:
            radar_name: Radar name (e.g., "RMA1")
            strategy: Volume strategy/code (e.g., "0315")
            vol_nr: Volume number (e.g., "01")
            observation_datetime: Observation datetime (ISO format)

        Returns:
            Unique volume identifier string
        """
        return f"{radar_name}_{strategy}_{vol_nr}_{observation_datetime}"

    def get_volume_info(self, volume_id: str) -> Optional[Dict]:
        """
        Get information about a volume.

        Args:
            volume_id: Volume identifier

        Returns:
            Dictionary with volume info, or None if not found
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM volume_processing WHERE volume_id = ?", (volume_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def register_volume(
        self,
        volume_id: str,
        radar_name: str,
        strategy: str,
        vol_nr: str,
        observation_datetime: str,
        expected_fields: List[str],
        is_complete: bool,
    ) -> None:
        """
        Register a new volume in the database.

        Args:
            volume_id: Unique volume identifier
            radar_name: Radar name
            strategy: Volume strategy/code
            vol_nr: Volume number
            observation_datetime: Observation datetime (ISO format)
            expected_fields: List of expected field types
            is_complete: Whether volume is complete
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        now = datetime.now(timezone.utc).isoformat()

        # Convert list to comma-separated string
        expected_fields_str = ",".join(expected_fields)

        cursor.execute(
            """
            INSERT INTO volume_processing
            (volume_id, radar_name, strategy, vol_nr, observation_datetime,
             status, is_complete, expected_fields, downloaded_fields, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, '', ?, ?)
        """,
            (
                volume_id,
                radar_name,
                strategy,
                vol_nr,
                observation_datetime,
                1 if is_complete else 0,
                expected_fields_str,
                now,
                now,
            ),
        )

        conn.commit()
        logger.debug(f"Registered volume '{volume_id}' (complete={is_complete})")

    def update_volume_fields(self, volume_id: str, downloaded_fields: List[str], is_complete: bool) -> None:
        """
        Update volume fields and completion status.

        Args:
            volume_id: Volume identifier
            downloaded_fields: List of downloaded field types
            is_complete: Whether volume is complete
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        now = datetime.now(timezone.utc).isoformat()

        # Convert list to comma-separated string
        downloaded_fields_str = ",".join(downloaded_fields)

        cursor.execute(
            """
            UPDATE volume_processing
            SET downloaded_fields = ?, is_complete = ?, updated_at = ?
            WHERE volume_id = ?
        """,
            (downloaded_fields_str, 1 if is_complete else 0, now, volume_id),
        )

        conn.commit()
        logger.debug(f"Updated volume '{volume_id}' fields (complete={is_complete})")

    def mark_volume_processing(
        self, volume_id: str, status: str, netcdf_path: Optional[str] = None, error_message: Optional[str] = None
    ) -> None:
        """
        Mark volume processing status.

        Args:
            volume_id: Volume identifier
            status: Processing status ('pending', 'processing', 'completed', 'failed')
            netcdf_path: Optional path to generated NetCDF file
            error_message: Optional error message if failed
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        now = datetime.now(timezone.utc).isoformat()

        updates = ["status = ?", "updated_at = ?"]
        params = [status, now]

        if netcdf_path:
            updates.append("netcdf_path = ?")
            params.append(netcdf_path)

        if error_message:
            updates.append("error_message = ?")
            params.append(error_message)

        if status == "completed":
            updates.append("processed_at = ?")
            params.append(now)

        params.append(volume_id)

        cursor.execute(
            f"""
            UPDATE volume_processing
            SET {', '.join(updates)}
            WHERE volume_id = ?
        """,
            tuple(params),
        )

        conn.commit()
        logger.debug(f"Marked volume '{volume_id}' as {status}")

    def get_complete_unprocessed_volumes(self) -> List[Dict]:
        """
        Get complete volumes that haven't been processed yet.

        Returns:
            List of volume info dictionaries
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT * FROM volume_processing
            WHERE is_complete = 1 AND status = 'pending'
            ORDER BY observation_datetime ASC
        """
        )

        return [dict(row) for row in cursor.fetchall()]

    def get_volume_files(self, radar_name: str, strategy: str, vol_nr: str, observation_datetime: str) -> List[Dict]:
        """
        Get all BUFR file information for a specific volume.

        Args:
            radar_name: Radar name
            strategy: Volume strategy/code
            vol_nr: Volume number
            observation_datetime: Observation datetime (ISO format)

        Returns:
            List of dictionaries with file information (filename, local_path, etc.)
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT filename, local_path, file_size, field_type FROM downloads
            WHERE radar_name = ? AND strategy = ? AND vol_nr = ?
            AND observation_datetime = ? AND status = 'completed'
        """,
            (radar_name, strategy, vol_nr, observation_datetime),
        )

        return [
            {
                "filename": row[0],
                "local_path": row[1],
                "file_size": row[2],
                "field_type": row[3],
            }
            for row in cursor.fetchall()
        ]

    def get_volumes_by_status(self, status: str) -> List[Dict]:
        """
        Get volumes by processing status.

        Args:
            status: Processing status to filter by

        Returns:
            List of volume info dictionaries
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT * FROM volume_processing
            WHERE status = ?
            ORDER BY observation_datetime DESC
        """,
            (status,),
        )

        return [dict(row) for row in cursor.fetchall()]

    def get_latest_registered_volume_datetime(self, radar_name: str) -> Optional[str]:
        """
        Get the observation datetime of the latest registered volume for a radar.

        Args:
            radar_name: Radar name to filter by

        Returns:
            ISO format datetime string of latest volume, or None if no volumes exist
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT observation_datetime FROM volume_processing
            WHERE radar_name = ?
            ORDER BY observation_datetime DESC
            LIMIT 1
        """,
            (radar_name,),
        )

        row = cursor.fetchone()
        return row[0] if row else None

    def get_unprocessed_volumes(self) -> List[Dict]:
        """
        Get all volumes that haven't been processed yet (both complete and incomplete).

        Returns:
            List of volume info dictionaries
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT * FROM volume_processing
            WHERE status = 'pending'
            ORDER BY observation_datetime ASC
        """
        )

        return [dict(row) for row in cursor.fetchall()]
