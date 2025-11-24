# -*- coding: utf-8 -*-
"""
Daemons module for radar data processing pipeline.

This module provides the core daemons for the radar data processing pipeline:

- **DownloadDaemon**: Monitors FTP server and downloads BUFR files
- **ProcessingDaemon**: Processes BUFR files and creates NetCDF volumes
- **ProductGenerationDaemon**: Generates visualization products (PNG, GeoTIFF) from NetCDF
- **DaemonManager**: Orchestrates all daemons for a complete pipeline

Example:
    >>> from radarlib.daemons import DaemonManager, DaemonManagerConfig
    >>> config = DaemonManagerConfig(...)
    >>> manager = DaemonManager(config)
    >>> await manager.start()
"""

# Main daemons
from radarlib.daemons.download_daemon import (  # Backward compatibility aliases
    ContinuousDaemon,
    ContinuousDaemonConfig,
    ContinuousDaemonError,
    DownloadDaemon,
    DownloadDaemonConfig,
    DownloadDaemonError,
)

# Legacy daemons (for backward compatibility)
from radarlib.daemons.legacy import DateBasedDaemonConfig, DateBasedFTPDaemon, FTPDaemon, FTPDaemonConfig
from radarlib.daemons.manager import DaemonManager, DaemonManagerConfig
from radarlib.daemons.processing_daemon import ProcessingDaemon, ProcessingDaemonConfig
from radarlib.daemons.product_daemon import ProductGenerationDaemon, ProductGenerationDaemonConfig

__all__ = [
    # Main daemons (new names)
    "DownloadDaemon",
    "DownloadDaemonConfig",
    "DownloadDaemonError",
    "ProcessingDaemon",
    "ProcessingDaemonConfig",
    "ProductGenerationDaemon",
    "ProductGenerationDaemonConfig",
    "DaemonManager",
    "DaemonManagerConfig",
    # Backward compatibility (old names)
    "ContinuousDaemon",
    "ContinuousDaemonConfig",
    "ContinuousDaemonError",
    # Legacy daemons
    "FTPDaemon",
    "FTPDaemonConfig",
    "DateBasedFTPDaemon",
    "DateBasedDaemonConfig",
]
