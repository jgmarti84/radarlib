# PNG Generation Daemon - Architecture and Usage

## Overview

The PNG Generation Daemon provides automatic monitoring and generation of PNG field plots from processed NetCDF radar volumes. It works alongside the Continuous Download Daemon and Processing Daemon to create a complete pipeline from FTP download to radar volume visualization.

## Key Features

1. ✅ Automatic monitoring of processed volumes
2. ✅ PNG field plot generation for all radar fields
3. ✅ COLMAX field generation and visualization
4. ✅ Works with both complete and incomplete volumes
5. ✅ PNG generation state tracking in SQLite database
6. ✅ Error handling and recovery
7. ✅ Concurrent volume processing
8. ✅ Stuck volume detection and retry

## Architecture

### 1. PNG Generation State Tracking

The `SQLiteStateTracker` has been extended with PNG generation status columns in the `volume_processing` table.

**Database Schema Extensions**:
```sql
volume_processing (extended):
  - png_status (pending, processing, completed, failed)
  - png_generated_at (timestamp when PNGs were generated)
  - png_error_message (error message if generation failed)
```

**New Methods**:
- `mark_volume_png_status()`: Mark PNG generation status
- `get_volumes_for_png_generation()`: Get volumes ready for PNG generation
- `get_volumes_by_png_status()`: Get volumes by PNG status
- `get_stuck_png_volumes()`: Get volumes stuck in processing
- `reset_stuck_png_volumes()`: Reset stuck volumes to pending

### 2. PNG Generation Daemon

The `PNGGenerationDaemon` class monitors processed NetCDF files and generates PNG plots.

**Workflow**:
```
1. Poll volume_processing table for volumes with:
   - status='completed' (NetCDF file exists)
   - png_status='pending' or 'failed'
2. For each volume:
   - Mark as "processing"
   - Read NetCDF file
   - Generate PNG plots for all fields (using process_volume logic)
   - Generate COLMAX field
   - Save PNGs to configured output directory
   - Mark as "completed" or "failed"
3. Sleep for poll_interval
4. Repeat
```

## Usage Examples

### Example 1: Basic PNG Generation Daemon

Generate PNGs for processed NetCDF files:

```python
import asyncio
from pathlib import Path
from radarlib.io.ftp import PNGGenerationDaemon, PNGGenerationDaemonConfig

# Define volume types (must match other daemons config)
volume_types = {
    "0315": {
        "01": ["DBZH", "DBZV", "ZDR", "RHOHV", "PHIDP", "KDP"],
        "02": ["VRAD", "WRAD"],
    },
}

config = PNGGenerationDaemonConfig(
    local_netcdf_dir=Path("./netcdf"),
    local_png_dir=Path("./png"),
    state_db=Path("./state.db"),
    volume_types=volume_types,
    radar_name="RMA1",
    poll_interval=30,  # Check every 30 seconds
    max_concurrent_processing=2,  # Process 2 volumes at once
    add_colmax=True,  # Generate COLMAX field
)

daemon = PNGGenerationDaemon(config)
await daemon.run()
```

### Example 2: Complete Pipeline with All Three Daemons

Run download, processing, and PNG generation together:

```python
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from radarlib.io.ftp import (
    ContinuousDaemon,
    ContinuousDaemonConfig,
    ProcessingDaemon,
    ProcessingDaemonConfig,
    PNGGenerationDaemon,
    PNGGenerationDaemonConfig,
)

volume_types = {
    "0315": {
        "01": ["DBZH", "DBZV", "ZDR", "RHOHV", "PHIDP", "KDP"],
        "02": ["VRAD", "WRAD"],
    },
}

# Download daemon config
download_config = ContinuousDaemonConfig(
    host="ftp.example.com",
    username="user",
    password="pass",
    remote_base_path="/L2",
    radar_name="RMA1",
    local_bufr_dir=Path("./bufr"),
    state_db=Path("./state.db"),
    start_date=datetime(2025, 11, 24, tzinfo=timezone.utc),
    vol_types=volume_types,
)

# Processing daemon config
processing_config = ProcessingDaemonConfig(
    local_bufr_dir=Path("./bufr"),
    local_netcdf_dir=Path("./netcdf"),
    state_db=Path("./state.db"),
    volume_types=volume_types,
    radar_name="RMA1",
)

# PNG generation daemon config
png_config = PNGGenerationDaemonConfig(
    local_netcdf_dir=Path("./netcdf"),
    local_png_dir=Path("./png"),
    state_db=Path("./state.db"),
    volume_types=volume_types,
    radar_name="RMA1",
    add_colmax=True,
)

async def run_pipeline():
    """Run all three daemons concurrently."""
    download_daemon = ContinuousDaemon(download_config)
    processing_daemon = ProcessingDaemon(processing_config)
    png_daemon = PNGGenerationDaemon(png_config)
    
    await asyncio.gather(
        download_daemon.run_service(),
        processing_daemon.run(),
        png_daemon.run(),
    )

asyncio.run(run_pipeline())
```

### Example 3: Using Daemon Manager

The easiest way to run all three daemons:

```python
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from radarlib.io.ftp.daemon_manager import DaemonManager, DaemonManagerConfig

volume_types = {
    "0315": {
        "01": ["DBZH", "DBZV", "ZDR", "RHOHV", "PHIDP", "KDP"],
        "02": ["VRAD", "WRAD"],
    },
}

manager_config = DaemonManagerConfig(
    radar_name="RMA1",
    base_path=Path("./data/RMA1"),
    ftp_host="ftp.example.com",
    ftp_user="user",
    ftp_password="pass",
    ftp_base_path="/L2",
    volume_types=volume_types,
    start_date=datetime(2025, 11, 24, tzinfo=timezone.utc),
    download_poll_interval=60,
    processing_poll_interval=30,
    png_poll_interval=30,
    enable_download_daemon=True,
    enable_processing_daemon=True,
    enable_png_daemon=True,
    add_colmax=True,
)

manager = DaemonManager(manager_config)
await manager.start()
```

### Example 4: Check PNG Generation Status

Query the database to see PNG generation progress:

```python
from pathlib import Path
from radarlib.io.ftp import SQLiteStateTracker

tracker = SQLiteStateTracker(Path("./state.db"))

# Get volumes by PNG status
pending = tracker.get_volumes_by_png_status("pending")
processing = tracker.get_volumes_by_png_status("processing")
completed = tracker.get_volumes_by_png_status("completed")
failed = tracker.get_volumes_by_png_status("failed")

print(f"Pending: {len(pending)}")
print(f"Processing: {len(processing)}")
print(f"Completed: {len(completed)}")
print(f"Failed: {len(failed)}")

# Get volumes ready for PNG generation
ready = tracker.get_volumes_for_png_generation()
print(f"Ready to process: {len(ready)}")

tracker.close()
```

## PNG Generation Process

The daemon uses the `process_volume` function from `vol_process.py` to generate PNGs, which:

1. **Reads NetCDF file**: Loads the radar volume using PyART
2. **Standardizes fields**: Ensures field names are consistent
3. **Determines reflectivity fields**: Identifies horizontal and vertical reflectivity
4. **Generates COLMAX** (if enabled): Creates column maximum field with filtering
5. **Plots unfiltered fields**: Generates PNG plots for all configured fields
6. **Plots filtered fields**: Applies quality control filters and generates filtered PNGs
7. **Saves outputs**: Stores PNGs in organized directory structure

### Output Directory Structure

```
png/
├── RMA1/
│   ├── 2025/
│   │   ├── 11/
│   │   │   ├── 24/
│   │   │   │   ├── RMA1_20251124T120000Z_DBZH_00.png
│   │   │   │   ├── RMA1_20251124T120000Z_DBZV_00.png
│   │   │   │   ├── RMA1_20251124T120000Z_colmax_00.png
│   │   │   │   └── ...
```

## Complete and Incomplete Volumes

The daemon processes **both complete and incomplete volumes**:

- **Complete volumes**: Have all expected fields for their volume type
- **Incomplete volumes**: Missing some expected fields but NetCDF was generated

This allows visualization even when some radar data is missing or corrupted.

## Error Handling

The daemon handles various error scenarios:

1. **Missing NetCDF file**: Volume marked but file doesn't exist
2. **Read errors**: NetCDF file is corrupted or unreadable
3. **Processing errors**: PNG generation fails due to data issues
4. **Disk space**: Output directory full or permissions issue

Failed volumes are:
- Marked with `png_status = 'failed'`
- Error message stored in `png_error_message` field
- Can be retried manually by resetting status

## Performance

- **Concurrent Processing**: Multiple volumes processed simultaneously (configurable)
- **Async Operations**: Non-blocking I/O for monitoring
- **Executor Threading**: PNG generation runs in separate threads to avoid blocking
- **Stuck Volume Detection**: Automatic reset of volumes stuck in processing

## Logging

The daemon logs extensively:

- Volume detection ready for PNG generation
- Processing start/completion
- Errors with full stack traces
- Statistics updates

**Log Levels**:
- `INFO`: Volume detection, PNG generation status
- `DEBUG`: File reading, detailed progress
- `ERROR`: Generation failures with tracebacks
- `WARNING`: Stuck volumes, unexpected conditions

## Integration with Other Daemons

The PNG generation daemon integrates seamlessly with existing daemons:

1. **Shared Database**: All daemons use the same `state.db` SQLite file
2. **Shared Config**: All use the same `volume_types` configuration
3. **Independent Operation**: Can run separately or together
4. **Data Flow**: Download → BUFR → Processing → NetCDF → PNG Generation → PNG files

## Monitoring and Statistics

Get daemon statistics:

```python
stats = daemon.get_stats()
print(stats)
# {
#     'running': True,
#     'volumes_processed': 42,
#     'volumes_failed': 3,
#     'pending_volumes': 5,
#     'completed_volumes': 42,
# }
```

## Configuration Reference

### PNGGenerationDaemonConfig

```python
@dataclass
class PNGGenerationDaemonConfig:
    local_netcdf_dir: Path              # Directory with NetCDF files
    local_png_dir: Path                 # Directory for PNG output
    state_db: Path                      # SQLite database (shared)
    volume_types: Dict[str, Dict]       # Expected field types per volume
    radar_name: str                     # Radar to process (e.g., "RMA1")
    poll_interval: int = 30             # Seconds between checks
    max_concurrent_processing: int = 2  # Max simultaneous volumes
    add_colmax: bool = True            # Generate COLMAX field
    stuck_volume_timeout_minutes: int = 60  # Timeout for stuck volumes
```

## Common Use Cases

### 1. Real-time Visualization
Run all three daemons continuously for real-time pipeline from FTP to PNG visualization.

### 2. Batch Processing
Run processing daemon to generate NetCDF files, then run PNG daemon to create visualizations.

### 3. Reprocessing
Reset PNG status in database and rerun PNG daemon:
```python
tracker.mark_volume_png_status(volume_id, "pending")
```

### 4. Monitoring
Query database periodically to check PNG generation progress and identify issues.

## Troubleshooting

### Volume Not Being Processed
- Check `status` is 'completed' in volume_processing table
- Verify `png_status` is 'pending' or 'failed'
- Check NetCDF file exists at `netcdf_path`

### PNG Generation Fails
- Check `png_error_message` in database
- Verify NetCDF file is valid and readable
- Check output directory permissions and disk space
- Review daemon logs for detailed error info

### No PNGs Generated
- Verify daemon is running
- Check `volume_types` configuration matches processed volumes
- Ensure output directory is writable

## See Also

- [PROCESSING_DAEMON.md](PROCESSING_DAEMON.md) - NetCDF processing daemon documentation
- [examples/png_daemon_example.py](examples/png_daemon_example.py) - Usage examples
- [src/radarlib/io/pyart/vol_process.py](src/radarlib/io/pyart/vol_process.py) - PNG generation logic
