"""
FTP module for interacting with FTP servers and managing BUFR file downloads.
"""

from .client import FTPClient
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
from .state_tracker import FileStateTracker

__all__ = [
    # Client and daemon
    "FTPClient",
    "FTPDaemon",
    "FTPDaemonConfig",
    "FileStateTracker",
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
