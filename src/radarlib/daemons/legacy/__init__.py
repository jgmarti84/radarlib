# -*- coding: utf-8 -*-
"""
Legacy daemons module.

This module contains older daemon implementations that are kept for backward compatibility.
Consider using the main daemons module for new development.
"""

from radarlib.daemons.legacy.date_daemon import DateBasedDaemonConfig, DateBasedFTPDaemon
from radarlib.daemons.legacy.ftp_daemon import FTPDaemon, FTPDaemonConfig

__all__ = [
    "FTPDaemon",
    "FTPDaemonConfig",
    "DateBasedFTPDaemon",
    "DateBasedDaemonConfig",
]
