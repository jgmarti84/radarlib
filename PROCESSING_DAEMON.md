# BUFR Processing Daemon - Architecture and Usage

## Overview

The BUFR Processing Daemon provides automatic monitoring and processing of downloaded BUFR files into NetCDF format. It works alongside the Date-Based FTP Daemon to create a complete pipeline from FTP download to radar volume processing.

## Key Features

1. ✅ Automatic volume completion detection
2. ✅ Complete volume processing (all required field types)
3. ✅ BUFR decoding and PyART Radar object creation
4. ✅ NetCDF file generation with CF/Radial convention
5. ✅ Processing state tracking in SQLite database
6. ✅ Error handling and recovery
7. ✅ Concurrent volume processing
8. ✅ Incomplete volume detection and tracking

## Architecture

### 1. Volume Processing State Tracker

The `SQLiteStateTracker` has been extended with a new `volume_processing` table to track the processing status of radar volumes.

**Database Schema**:
```sql
volume_processing:
  - volume_id (unique identifier: RADAR_VOLCODE_VOLNUM_DATETIME)
  - radar_code, vol_code, vol_number
  - observation_datetime
  - status (pending, processing, completed, failed)
  - netcdf_path (path to generated NetCDF file)
  - is_complete (whether all expected fields are downloaded)
  - expected_fields (comma-separated list)
  - downloaded_fields (comma-separated list)
  - error_message (if processing failed)
```

**Key Methods**:
- `register_volume()`: Register a new volume for processing
- `update_volume_fields()`: Update downloaded fields and completion status
- `mark_volume_processing()`: Mark volume status (processing, completed, failed)
- `get_complete_unprocessed_volumes()`: Get complete volumes ready for processing
- `get_volume_files()`: Get all BUFR files belonging to a volume

### 2. Processing Daemon

The `ProcessingDaemon` class monitors downloaded BUFR files and processes complete volumes.

**Workflow**:
```
1. Scan downloads table for files
2. Group files by volume (radar + vol_code + vol_num + timestamp)
3. Check if volume is complete (has all expected field types)
4. Register/update volume in volume_processing table
5. For complete volumes:
   - Mark as "processing"
   - Decode all BUFR files
   - Create PyART Radar object
   - Save as NetCDF file
   - Mark as "completed" or "failed"
6. Sleep for poll_interval
7. Repeat
```

## Usage Examples

### Example 1: Basic Processing Daemon

Process downloaded BUFR files automatically:

```python
import asyncio
from pathlib import Path
from radarlib.io.ftp import ProcessingDaemon, ProcessingDaemonConfig

# Define volume types (must match download daemon config)
volume_types = {
    "0315": {
        "01": ["DBZH", "DBZV", "ZDR", "RHOHV", "PHIDP", "KDP"],
        "02": ["VRAD", "WRAD"],
    },
}

config = ProcessingDaemonConfig(
    local_bufr_dir=Path("./downloads/RMA1/bufr"),
    local_netcdf_dir=Path("./downloads/RMA1/netcdf"),
    state_db=Path("./downloads/RMA1/state.db"),
    volume_types=volume_types,
    radar_code="RMA1",
    poll_interval=30,  # Check every 30 seconds
    max_concurrent_processing=2,  # Process 2 volumes at once
)

daemon = ProcessingDaemon(config)
await daemon.run()
```

### Example 2: Combined Download and Processing Pipeline

Run both daemons together for complete automation:

```python
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from radarlib.io.ftp import (
    DateBasedFTPDaemon,
    DateBasedDaemonConfig,
    ProcessingDaemon,
    ProcessingDaemonConfig,
)

volume_types = {
    "0315": {
        "01": ["DBZH", "DBZV", "ZDR", "RHOHV", "PHIDP", "KDP"],
        "02": ["VRAD", "WRAD"],
    },
}

# Download daemon config
download_config = DateBasedDaemonConfig(
    host="ftp.example.com",
    username="user",
    password="pass",
    remote_base_path="/L2",
    radar_code="RMA1",
    local_download_dir=Path("./downloads/RMA1/bufr"),
    state_db=Path("./downloads/RMA1/state.db"),
    start_date=datetime(2025, 11, 17, tzinfo=timezone.utc),
    end_date=None,  # Continuous
    volume_types=volume_types,
)

# Processing daemon config (shares same state.db)
processing_config = ProcessingDaemonConfig(
    local_bufr_dir=Path("./downloads/RMA1/bufr"),
    local_netcdf_dir=Path("./downloads/RMA1/netcdf"),
    state_db=Path("./downloads/RMA1/state.db"),
    volume_types=volume_types,
    radar_code="RMA1",
)

async def run_pipeline():
    """Run both daemons concurrently."""
    download_daemon = DateBasedFTPDaemon(download_config)
    processing_daemon = ProcessingDaemon(processing_config)

    await asyncio.gather(
        download_daemon.run(),
        processing_daemon.run(),
    )

asyncio.run(run_pipeline())
```

### Example 3: Check Processing Status

Query the database to see processing status:

```python
from pathlib import Path
from radarlib.io.ftp import SQLiteStateTracker

tracker = SQLiteStateTracker(Path("./downloads/RMA1/state.db"))

# Get volumes by status
pending = tracker.get_volumes_by_status("pending")
processing = tracker.get_volumes_by_status("processing")
completed = tracker.get_volumes_by_status("completed")
failed = tracker.get_volumes_by_status("failed")

print(f"Pending: {len(pending)}")
print(f"Processing: {len(processing)}")
print(f"Completed: {len(completed)}")
print(f"Failed: {len(failed)}")

# Get complete volumes ready for processing
ready = tracker.get_complete_unprocessed_volumes()
print(f"Ready to process: {len(ready)}")

# Get details of a specific volume
if ready:
    volume = ready[0]
    print(f"\nVolume: {volume['volume_id']}")
    print(f"  Radar: {volume['radar_code']}")
    print(f"  Vol Code/Num: {volume['vol_code']}/{volume['vol_number']}")
    print(f"  Time: {volume['observation_datetime']}")
    print(f"  Expected fields: {volume['expected_fields']}")
    print(f"  Downloaded fields: {volume['downloaded_fields']}")
    print(f"  Complete: {volume['is_complete'] == 1}")

tracker.close()
```

## Volume Completion Logic

A volume is considered **complete** when all expected field types for a specific `vol_code` and `vol_number` combination have been downloaded.

**Example**:
```python
volume_types = {
    "0315": {
        "01": ["DBZH", "DBZV"],  # Volume 0315/01 needs 2 fields
        "02": ["VRAD", "WRAD"],  # Volume 0315/02 needs 2 fields
    }
}
```

For radar RMA1 at timestamp 2025-11-18T12:30:00Z:
- Volume `RMA1_0315_01_2025-11-18T12:30:00Z` is complete when both:
  - `RMA1_0315_01_DBZH_20251118T123000Z.BUFR`
  - `RMA1_0315_01_DBZV_20251118T123000Z.BUFR`

  are downloaded.

## Incomplete Volumes

Volumes that don't have all expected fields are marked as incomplete:
- `is_complete = 0` in database
- Not processed automatically
- Tracked for debugging/monitoring purposes

**Configuration Options**:
- `allow_incomplete`: Process incomplete volumes (not yet implemented)
- `incomplete_timeout_hours`: Wait time before processing incomplete volumes (not yet implemented)

## Error Handling

The daemon handles various error scenarios:

1. **Missing Files**: If files are registered but don't exist locally
2. **Decoding Errors**: If BUFR files can't be decoded
3. **Processing Errors**: If PyART Radar creation or NetCDF saving fails

Failed volumes are:
- Marked with `status = 'failed'`
- Error message stored in `error_message` field
- Can be retried manually or by resetting status

## Performance

- **Concurrent Processing**: Multiple volumes processed simultaneously (configurable)
- **Async Operations**: Non-blocking I/O for monitoring
- **SQLite Efficiency**: Indexed queries for fast volume lookups
- **Executor Threading**: BUFR decoding runs in separate threads to avoid blocking

## Logging

The daemon logs extensively:
- Volume detection (complete/incomplete)
- Processing start/completion
- Errors with full stack traces
- Statistics updates

**Log Levels**:
- `INFO`: Volume completion, processing status
- `DEBUG`: File scanning, volume updates
- `ERROR`: Processing failures with tracebacks
- `WARNING`: Parsing errors, unexpected conditions

## Integration with Download Daemon

The processing daemon is designed to work seamlessly with the Date-Based FTP Daemon:

1. **Shared Database**: Both use the same `state.db` SQLite file
2. **Shared Config**: Both use the same `volume_types` configuration
3. **Independent Operation**: Can run separately or together
4. **Data Flow**: Download → Database → Processing → NetCDF

## Monitoring and Statistics

Get daemon statistics:

```python
stats = daemon.get_stats()
print(stats)
# {
#     'running': True,
#     'volumes_processed': 42,
#     'volumes_failed': 3,
#     'incomplete_volumes_detected': 7,
#     'pending_volumes': 2,
#     'complete_unprocessed': 2,
# }
```

## File Naming

**Input (BUFR)**:
```
RMA1_0315_01_DBZH_20251118T123000Z.BUFR
```

**Output (NetCDF)**:
```
RMA1_0315_01_20251118T123000Z.nc
```

The NetCDF filename is generated from the first BUFR file in the volume, with the field type removed.

## Configuration Reference

### ProcessingDaemonConfig

```python
@dataclass
class ProcessingDaemonConfig:
    local_bufr_dir: Path              # Directory with downloaded BUFR files
    local_netcdf_dir: Path            # Directory for NetCDF output
    state_db: Path                    # SQLite database (shared with download daemon)
    volume_types: Dict[str, Dict]     # Expected field types per volume
    radar_code: str                   # Radar to process (e.g., "RMA1")
    poll_interval: int = 30           # Seconds between checks
    max_concurrent_processing: int = 2 # Max simultaneous volumes
    root_resources: Optional[Path] = None  # BUFR decoder resources
    allow_incomplete: bool = False    # Process incomplete volumes
    incomplete_timeout_hours: int = 24 # Hours to wait for incomplete
```

## Common Use Cases

### 1. Real-time Processing
Run both daemons continuously for real-time pipeline from FTP to NetCDF.

### 2. Batch Processing
Run download daemon to get files, then run processing daemon to generate NetCDF.

### 3. Reprocessing
Reset volume status in database and rerun processing daemon:
```python
tracker.mark_volume_processing(volume_id, "pending")
```

### 4. Monitoring
Query database periodically to check processing status and identify issues.

## Troubleshooting

### Volume Not Being Processed
- Check `is_complete` flag in database
- Verify all expected fields are downloaded
- Check `volume_types` configuration matches downloaded files

### Processing Fails
- Check error_message in database
- Verify BUFR files are valid and readable
- Check PyART installation and dependencies
- Review daemon logs for detailed error info

### Incomplete Volumes
- May indicate missing files on FTP server
- Check download daemon logs
- Verify `volume_types` configuration is correct

## Future Enhancements

Potential features for future development:
- [ ] Automatic retry for failed volumes
- [ ] Processing of incomplete volumes after timeout
- [ ] Quality control checks before processing
- [ ] Parallel BUFR decoding within a volume
- [ ] Compression of NetCDF files
- [ ] Automatic cleanup of old BUFR files
- [ ] Email/webhook notifications on completion/failure
- [ ] Web dashboard for monitoring

## See Also

- [DATE_BASED_DAEMON.md](DATE_BASED_DAEMON.md) - Download daemon documentation
- [examples/processing_daemon_example.py](examples/processing_daemon_example.py) - Usage examples
- [tests/integration/test_processing_daemon_integration.py](tests/integration/test_processing_daemon_integration.py) - Integration tests
