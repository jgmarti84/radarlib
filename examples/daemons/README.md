# Daemon Examples

This folder contains examples for using the radarlib daemon system.

## Main Daemons

- **download_daemon_example.py** - Download BUFR files from FTP server
- **processing_daemon_example.py** - Process BUFR files into NetCDF volumes
- **product_daemon_example.py** - Generate PNG visualizations from NetCDF
- **daemon_manager_example.py** - Orchestrate all daemons together

## Legacy Daemons

- **ftp_daemon_example.py** - Legacy FTP daemon (consider using DownloadDaemon instead)
- **ftp_date_daemon_example.py** - Legacy date-based daemon

## Usage

```python
# New recommended imports
from radarlib.daemons import DownloadDaemon, ProcessingDaemon, ProductGenerationDaemon
from radarlib.daemons import DaemonManager, DaemonManagerConfig
```
