# radarlib

## Overview

My Python Library is a Python package designed to provide essential functionalities for managing radars and radar products.

## Features

- Easy-to-use API for common tasks
- Comprehensive unit tests to ensure reliability
- Built-in support for debugging and development
- **Async FTP Daemon Service**: Background service for continuous monitoring and downloading of BUFR files from FTP servers
- BUFR file decoding and processing
- PyART radar data integration
- PNG and GeoTIFF export capabilities

## Installation

To install the library, you can use pip:

```bash
pip install radarlib
```

For development purposes, clone the repository and install the dependencies:

```bash
git clone https://github.com/yourusername/my-python-library.git
cd my-python-library
pip install -r requirements.txt
```

## Usage

### FTP Daemon Service

The FTP daemon service allows you to continuously monitor an FTP server for new BUFR files:

```python
import asyncio
from radarlib.io.ftp import FTPDaemon, FTPDaemonConfig

async def run_daemon():
    config = FTPDaemonConfig(
        host='ftp.example.com',
        username='user',
        password='pass',
        remote_path='/radar/data',
        local_dir='/data/radar/incoming',
        poll_interval=60,  # Check every 60 seconds
        max_concurrent_downloads=5
    )

    daemon = FTPDaemon(config)
    await daemon.run()  # Runs indefinitely

asyncio.run(run_daemon())
```

For more examples, see:
- `examples/ftp_client_example.py` - Basic FTP client usage
- `examples/ftp_daemon_example.py` - Daemon service examples
- `examples/ftp_integration_example.py` - Complete integration with BUFR processing

### BUFR File Processing

Here is an example of how to process BUFR radar files:

```python
from radarlib.io.bufr import bufr_to_dict, bufr_to_pyart

# Decode BUFR file
bufr_dict = bufr_to_dict('radar_file.BUFR')

# Convert to PyART radar object
radar = bufr_to_pyart([bufr_dict])
```

## Development

To contribute to the development of this library, please follow these steps:

1. Clone the repository.
2. Create a new branch for your feature or bug fix.
3. Make your changes and ensure that all tests pass.
4. Submit a pull request.

## Testing

To run the tests, use the following command:

```bash
pytest
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
