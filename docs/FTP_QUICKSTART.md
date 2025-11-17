# Quick Start Guide: FTP Daemon Service

This guide will help you get started with the FTP daemon service for monitoring and downloading BUFR radar files.

## Installation

```bash
pip install radarlib
```

Or for development:
```bash
git clone https://github.com/jgmarti84/radarlib
cd radarlib
pip install -r requirements-dev.txt
pip install -e .
```

## Basic Usage

### 1. Simple File Download

Download files from an FTP server once:

```python
import asyncio
from radarlib.io.ftp import AsyncFTPClient

async def download_once():
    async with AsyncFTPClient('ftp.example.com', 'user', 'pass') as client:
        # List files
        files = await client.list_files('/radar/data', pattern='*.BUFR')
        print(f"Found {len(files)} files")
        
        # Download files
        downloaded = await client.download_files(
            files[:5],  # First 5 files
            '/local/data',
            max_concurrent=3
        )
        print(f"Downloaded {len(downloaded)} files")

asyncio.run(download_once())
```

### 2. Continuous Monitoring (Daemon)

Monitor an FTP server continuously for new files:

```python
import asyncio
from radarlib.io.ftp import FTPDaemon, FTPDaemonConfig

async def run_daemon():
    config = FTPDaemonConfig(
        host='ftp.example.com',
        username='user',
        password='pass',
        remote_path='/radar/data',
        local_dir='/local/data',
        poll_interval=60,  # Check every 60 seconds
        max_concurrent_downloads=5
    )
    
    daemon = FTPDaemon(config)
    await daemon.run()  # Runs indefinitely

# Run the daemon
asyncio.run(run_daemon())
```

### 3. Processing Files as They Arrive

Process files immediately after download:

```python
import asyncio
from radarlib.io.ftp import FTPDaemon, FTPDaemonConfig
from radarlib.io.bufr import bufr_to_dict

def process_file(local_path: str):
    """Process each downloaded file"""
    print(f"Processing: {local_path}")
    
    # Decode BUFR file
    bufr_dict = bufr_to_dict(local_path)
    
    # Process the data...
    print(f"✓ Processed {local_path}")

async def run_with_processing():
    config = FTPDaemonConfig(
        host='ftp.example.com',
        username='user',
        password='pass',
        remote_path='/radar/data',
        local_dir='/local/data',
        poll_interval=30
    )
    
    # Create daemon with callback
    daemon = FTPDaemon(config, on_file_downloaded=process_file)
    await daemon.run()

asyncio.run(run_with_processing())
```

## Production Deployment

### As a Python Script

Save this as `radar_daemon.py`:

```python
#!/usr/bin/env python3
import asyncio
import logging
import os
from radarlib.io.ftp import FTPDaemon, FTPDaemonConfig

logging.basicConfig(level=logging.INFO)

async def main():
    config = FTPDaemonConfig(
        host=os.getenv('FTP_HOST', 'ftp.example.com'),
        username=os.getenv('FTP_USER'),
        password=os.getenv('FTP_PASS'),
        remote_path=os.getenv('FTP_REMOTE_PATH', '/radar/data'),
        local_dir=os.getenv('LOCAL_DIR', '/data/radar'),
        poll_interval=int(os.getenv('POLL_INTERVAL', '60')),
        max_concurrent_downloads=int(os.getenv('MAX_DOWNLOADS', '5'))
    )
    
    daemon = FTPDaemon(config)
    await daemon.run()

if __name__ == '__main__':
    asyncio.run(main())
```

Run it:
```bash
export FTP_HOST=ftp.example.com
export FTP_USER=myuser
export FTP_PASS=mypass
python radar_daemon.py
```

### As a Systemd Service

Create `/etc/systemd/system/radar-ftp-daemon.service`:

```ini
[Unit]
Description=Radar FTP Daemon Service
After=network.target

[Service]
Type=simple
User=radar
WorkingDirectory=/opt/radar
Environment=FTP_HOST=ftp.example.com
Environment=FTP_USER=myuser
Environment=FTP_PASS=mypass
Environment=FTP_REMOTE_PATH=/radar/data
Environment=LOCAL_DIR=/data/radar/incoming
ExecStart=/usr/bin/python3 /opt/radar/radar_daemon.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl enable radar-ftp-daemon
sudo systemctl start radar-ftp-daemon
sudo systemctl status radar-ftp-daemon
```

View logs:
```bash
sudo journalctl -u radar-ftp-daemon -f
```

## Configuration Options

### FTPDaemonConfig Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `host` | str | required | FTP server hostname |
| `username` | str | required | FTP username |
| `password` | str | required | FTP password |
| `remote_path` | str | required | Remote directory to monitor |
| `local_dir` | str | required | Local directory for downloads |
| `port` | int | 21 | FTP port |
| `file_pattern` | str | '*.BUFR' | File pattern to match |
| `poll_interval` | int | 60 | Seconds between checks |
| `max_concurrent_downloads` | int | 5 | Max concurrent downloads |
| `recursive` | bool | False | Search recursively |

## Common Patterns

### 1. Error Handling with Retries

```python
async def download_with_retry(client, file_path, local_dir, max_retries=3):
    for attempt in range(max_retries):
        try:
            return await client.download_file(file_path, local_dir)
        except Exception as e:
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
            else:
                raise
```

### 2. Graceful Shutdown

```python
import signal

daemon = None

def signal_handler(signum, frame):
    if daemon:
        daemon.stop()

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

daemon = FTPDaemon(config)
await daemon.run()
```

### 3. Monitoring Progress

```python
class ProgressMonitor:
    def __init__(self):
        self.count = 0
        
    def on_download(self, path: str):
        self.count += 1
        print(f"Progress: {self.count} files downloaded")

monitor = ProgressMonitor()
daemon = FTPDaemon(config, on_file_downloaded=monitor.on_download)
```

## Troubleshooting

### Connection Issues

1. **Timeout**: Increase poll_interval
2. **Authentication**: Verify username/password
3. **Network**: Check firewall rules for port 21

### Performance Issues

1. **Slow downloads**: Increase max_concurrent_downloads
2. **High CPU**: Decrease max_concurrent_downloads
3. **Memory issues**: Process files in callback rather than accumulating

### File Processing Issues

1. **Duplicate downloads**: Check that local_dir is persistent
2. **Missing files**: Verify remote_path and file_pattern
3. **Processing errors**: Add error handling in callback

## More Examples

See the `examples/` directory for complete examples:
- `ftp_client_example.py` - FTP client usage
- `ftp_daemon_example.py` - Daemon configurations
- `ftp_integration_example.py` - Complete integration

## API Documentation

Full API documentation is available in `src/radarlib/io/ftp/README.md`

## Support

For issues or questions, please open an issue on GitHub.
