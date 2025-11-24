"""
FTP module for interacting with FTP servers.

This module provides FTP client functionality for connecting to and downloading
files from FTP servers.

Note: This module also re-exports daemon and state tracker classes for backward
      compatibility. For new code, import from their primary locations:
      - Daemons: `from radarlib.daemons import DownloadDaemon, ProcessingDaemon, ...`
      - State trackers: `from radarlib.state import SQLiteStateTracker, FileStateTracker`
"""

from .client import FTPClient

# Keep old files for backward compatibility during transition
# These will be imported from their original locations
from .continuous_daemon import ContinuousDaemon, ContinuousDaemonConfig
from .daemon import FTPDaemon, FTPDaemonConfig
from .daemon_manager import DaemonManager, DaemonManagerConfig
from .date_daemon import DateBasedDaemonConfig, DateBasedFTPDaemon
from .ftp import (
    FTP_IsADirectoryError,
    FTPActionError,
    build_ftp_path,
    download_file_from_ftp,
    download_ftp_folder,
    download_multiple_files_from_ftp,
    exponential_backoff_retry,
    ftp_connection_manager,
    list_files_in_remote_dir,
    parse_ftp_path,
)
from .ftp_client import FTPError, RadarFTPClientAsync
from .processing_daemon import ProcessingDaemon, ProcessingDaemonConfig
from .product_daemon import ProductGenerationDaemon, ProductGenerationDaemonConfig
from .sqlite_state_tracker import SQLiteStateTracker
from .state_tracker import FileStateTracker

__all__ = [
    # FTP clients
    "FTPClient",
    "RadarFTPClientAsync",
    "FTPError",
    # Main daemons (preferred location: radarlib.daemons)
    "ProcessingDaemon",
    "ProcessingDaemonConfig",
    "ProductGenerationDaemon",
    "ProductGenerationDaemonConfig",
    "DaemonManager",
    "DaemonManagerConfig",
    # Legacy daemon names (backward compatibility)
    "ContinuousDaemon",
    "ContinuousDaemonConfig",
    "FTPDaemon",
    "FTPDaemonConfig",
    "DateBasedFTPDaemon",
    "DateBasedDaemonConfig",
    # State trackers (preferred location: radarlib.state)
    "FileStateTracker",
    "SQLiteStateTracker",
    # FTP utility functions
    "ftp_connection_manager",
    "list_files_in_remote_dir",
    "download_file_from_ftp",
    "download_multiple_files_from_ftp",
    "download_ftp_folder",
    "build_ftp_path",
    "parse_ftp_path",
    "exponential_backoff_retry",
    # Exceptions
    "FTPActionError",
    "FTP_IsADirectoryError",
]
