# -*- coding: utf-8 -*-
"""State tracking for downloaded BUFR files."""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class FileStateTracker:
    """
    Track which BUFR files have been downloaded.

    Uses a JSON file to persist state between daemon restarts.
    Each entry stores: filename, download timestamp, remote path, and file metadata.

    Example:
        >>> tracker = FileStateTracker("./download_state.json")
        >>> tracker.mark_downloaded("file.BUFR", "/remote/path/file.BUFR")
        >>> if not tracker.is_downloaded("file2.BUFR"):
        ...     # Download file2.BUFR
        ...     tracker.mark_downloaded("file2.BUFR", "/remote/path/file2.BUFR")
    """

    def __init__(self, state_file: Path):
        """
        Initialize the state tracker.

        Args:
            state_file: Path to JSON file for persisting state
        """
        self.state_file = Path(state_file)
        self._state: Dict[str, Dict] = {}
        self._load_state()

    def _load_state(self) -> None:
        """Load state from JSON file."""
        if self.state_file.exists():
            try:
                with open(self.state_file, "r") as f:
                    self._state = json.load(f)
                logger.info(f"Loaded state with {len(self._state)} entries from {self.state_file}")
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Failed to load state file: {e}. Starting with empty state.")
                self._state = {}
        else:
            logger.info("No existing state file found. Starting with empty state.")
            self._state = {}

    def _save_state(self) -> None:
        """Save current state to JSON file."""
        try:
            # Ensure parent directory exists
            self.state_file.parent.mkdir(parents=True, exist_ok=True)

            with open(self.state_file, "w") as f:
                json.dump(self._state, f, indent=2)
            logger.debug(f"Saved state with {len(self._state)} entries")
        except IOError as e:
            logger.error(f"Failed to save state file: {e}")

    def is_downloaded(self, filename: str) -> bool:
        """
        Check if a file has been downloaded.

        Args:
            filename: Name of the file to check

        Returns:
            True if file has been downloaded, False otherwise
        """
        return filename in self._state

    def mark_downloaded(self, filename: str, remote_path: str, metadata: Optional[Dict] = None) -> None:
        """
        Mark a file as downloaded.

        Args:
            filename: Name of the downloaded file
            remote_path: Full remote path where file was located
            metadata: Optional metadata about the file (radar, field, timestamp, etc.)
        """
        self._state[filename] = {
            "remote_path": remote_path,
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
            "metadata": metadata or {},
        }
        self._save_state()
        logger.debug(f"Marked '{filename}' as downloaded")

    def get_downloaded_files(self) -> Set[str]:
        """
        Get set of all downloaded filenames.

        Returns:
            Set of filenames that have been downloaded
        """
        return set(self._state.keys())

    def get_file_info(self, filename: str) -> Optional[Dict]:
        """
        Get information about a downloaded file.

        Args:
            filename: Name of the file

        Returns:
            Dictionary with download info, or None if not found
        """
        return self._state.get(filename)

    def clear(self) -> None:
        """Clear all state (useful for testing or reset)."""
        self._state = {}
        self._save_state()
        logger.info("Cleared all state")

    def remove_file(self, filename: str) -> None:
        """
        Remove a file from the state (e.g., if download failed).

        Args:
            filename: Name of the file to remove
        """
        if filename in self._state:
            del self._state[filename]
            self._save_state()
            logger.debug(f"Removed '{filename}' from state")

    def get_files_by_date_range(self, start_date: datetime, end_date: datetime) -> List[str]:
        """
        Get files downloaded within a date range.

        Args:
            start_date: Start of date range
            end_date: End of date range

        Returns:
            List of filenames downloaded in the range
        """
        result = []
        for filename, info in self._state.items():
            downloaded_at = datetime.fromisoformat(info["downloaded_at"])
            if start_date <= downloaded_at <= end_date:
                result.append(filename)
        return result

    def count(self) -> int:
        """Get total number of downloaded files tracked."""
        return len(self._state)
