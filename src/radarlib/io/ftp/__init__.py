"""
FTP module for interacting with FTP servers and managing BUFR file downloads.
"""

from .client import FTPClient

# from .daemon_manager import DaemonManager, DaemonManagerConfig
# from .date_daemon import DateBasedDaemonConfig, DateBasedFTPDaemon
from .continuous_daemon import ContinuousDaemon, ContinuousDaemonConfig
from .daemon import FTPDaemon, FTPDaemonConfig
from .ftp import (
    FTP_IsADirectoryError,
    FTPActionError,
    build_ftp_path,
    download_file_from_ftp,
    download_ftp_folder,
    download_multiple_files_from_ftp,
    ftp_connection_manager,
    list_files_in_remote_dir,
    parse_ftp_path,
)
from .product_daemon import ProductGenerationDaemon, ProductGenerationDaemonConfig
from .processing_daemon import ProcessingDaemon, ProcessingDaemonConfig
from .sqlite_state_tracker import SQLiteStateTracker
from .state_tracker import FileStateTracker

__all__ = [
    # Client and daemon
    "FTPClient",
    "FTPDaemon",
    "FTPDaemonConfig",
    "ContinuousDaemon",
    "ContinuousDaemonConfig",
    # "DateBasedFTPDaemon",
    # "DateBasedDaemonConfig",
    "ProcessingDaemon",
    "ProcessingDaemonConfig",
    "ProductGenerationDaemon",
    "ProductGenerationDaemonConfig",
    # "DaemonManager",
    # "DaemonManagerConfig",
    "FileStateTracker",
    "SQLiteStateTracker",
    # Legacy functions
    "ftp_connection_manager",
    "list_files_in_remote_dir",
    "download_file_from_ftp",
    "download_multiple_files_from_ftp",
    "download_ftp_folder",
    "build_ftp_path",
    "parse_ftp_path",
    # Exceptions
    "FTPActionError",
    "FTP_IsADirectoryError",
]
