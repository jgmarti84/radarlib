# FTP Module for Radar Data

This module provides async FTP client functionality for fetching BUFR radar files from FTP servers.

## Features

- **Async FTP Client**: Built on `aioftp` for true asynchronous operations
- **Daemon Service**: Background service that continuously monitors FTP servers for new files
- **Concurrent Downloads**: Download multiple files simultaneously with configurable limits
- **Smart File Tracking**: Avoids re-downloading already processed files
- **Callback Support**: Process files immediately after download
- **Error Handling**: Robust error handling and retry capabilities

## Installation

The FTP module is included with radarlib. Make sure you have the required dependencies:

```bash
pip install radarlib
# or for development
pip install -r requirements-dev.txt
```

## Quick Start

### Basic FTP Client Usage

```python
import asyncio
from radarlib.io.ftp import AsyncFTPClient

async def download_files():
    # Connect to FTP server and download files
    async with AsyncFTPClient('ftp.example.com', 'user', 'pass') as client:
        # List BUFR files
        files = await client.list_files('/radar/data', pattern='*.BUFR')

        # Download files
        downloaded = await client.download_files(files[:5], '/local/data', max_concurrent=3)
        print(f"Downloaded {len(downloaded)} files")

asyncio.run(download_files())
```

### FTP Daemon Service

```python
import asyncio
from radarlib.io.ftp import FTPDaemon, FTPDaemonConfig

async def run_daemon():
    # Configure the daemon
    config = FTPDaemonConfig(
        host='ftp.example.com',
        username='user',
        password='pass',
        remote_path='/radar/data',
        local_dir='/data/radar/incoming',
        poll_interval=60,  # Check every 60 seconds
        max_concurrent_downloads=5
    )

    # Create and run daemon
    daemon = FTPDaemon(config)
    await daemon.run()  # Runs indefinitely

asyncio.run(run_daemon())
```

### Daemon with File Processing

```python
from radarlib.io.ftp import FTPDaemon, FTPDaemonConfig
from radarlib.io.bufr import bufr_to_dict

def process_file(local_path: str):
    """Process each downloaded file"""
    print(f"Processing: {local_path}")
    # Decode BUFR file
    bufr_dict = bufr_to_dict(local_path)
    # ... process the data ...

async def run_with_processing():
    config = FTPDaemonConfig(
        host='ftp.example.com',
        username='user',
        password='pass',
        remote_path='/radar/data',
        local_dir='/data/radar/incoming',
        poll_interval=30
    )

    # Create daemon with callback
    daemon = FTPDaemon(config, on_file_downloaded=process_file)
    await daemon.run()

asyncio.run(run_with_processing())
```

## API Reference

### AsyncFTPClient

Async FTP client for downloading files.

**Methods:**
- `connect()`: Connect to FTP server
- `disconnect()`: Disconnect from FTP server
- `list_files(remote_path, pattern='*.BUFR', recursive=False)`: List files matching pattern
- `download_file(remote_path, local_dir)`: Download a single file
- `download_files(remote_files, local_dir, max_concurrent=5)`: Download multiple files concurrently
- `check_file_exists(remote_path)`: Check if a file exists

### FTPDaemon

Async daemon service for monitoring FTP server.

**Methods:**
- `run(max_iterations=None)`: Start the daemon (runs indefinitely if max_iterations is None)
- `run_once()`: Check and download files once
- `stop()`: Stop the daemon
- `get_processed_files()`: Get list of processed files

### FTPDaemonConfig

Configuration for the FTP daemon.

**Parameters:**
- `host`: FTP server hostname
- `username`: FTP username
- `password`: FTP password
- `remote_path`: Remote directory to monitor
- `local_dir`: Local directory to save files
- `port`: FTP port (default: 21)
- `file_pattern`: File pattern to match (default: '*.BUFR')
- `poll_interval`: Seconds between checks (default: 60)
- `max_concurrent_downloads`: Max concurrent downloads (default: 5)
- `recursive`: Search recursively (default: False)

## Examples

See the `examples/` directory for complete working examples:

- `ftp_client_example.py`: Basic FTP client usage
- `ftp_daemon_example.py`: Daemon service examples

## Testing

Run the FTP module tests:

```bash
pytest tests/unit/test_ftp_client.py
pytest tests/unit/test_ftp_daemon.py
```

## Production Deployment

For production deployment as a systemd service:

1. Create a configuration file with your FTP credentials
2. Create a systemd service file
3. Enable and start the service

Example systemd service file (`/etc/systemd/system/radar-ftp-daemon.service`):

```ini
[Unit]
Description=Radar FTP Daemon Service
After=network.target

[Service]
Type=simple
User=radar
WorkingDirectory=/opt/radar
ExecStart=/usr/bin/python3 -m radar_ftp_daemon
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

## Error Handling

The FTP module includes comprehensive error handling:

- Connection failures are logged and can be retried
- Partial download failures don't stop other downloads
- Callback errors are caught and logged without stopping the daemon
- Network interruptions are handled gracefully

## Performance Considerations

- Use `max_concurrent_downloads` to control bandwidth usage
- Adjust `poll_interval` based on how frequently new files appear
- For large numbers of files, consider increasing concurrent downloads
- Monitor disk space in the local directory

## License

MIT License - see LICENSE file for details
