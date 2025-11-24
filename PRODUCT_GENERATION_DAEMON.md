# Product Generation Daemon - Architecture and Usage

## Overview

The Product Generation Daemon provides automatic monitoring and generation of visualization products (PNG plots, GeoTIFF, etc.) from processed NetCDF radar volumes. It works alongside the Continuous Download Daemon and Processing Daemon to create a complete pipeline from FTP download to radar volume visualization.

## Key Features

1. ✅ Automatic monitoring of processed volumes
2. ✅ Extensible product generation (PNG, GeoTIFF, future formats)
3. ✅ Separate `product_generation` table for tracking
4. ✅ PNG field plot generation for all radar fields
5. ✅ COLMAX field generation and visualization
6. ✅ Works with both complete and incomplete volumes
7. ✅ Comprehensive error tracking with error types
8. ✅ Error handling and recovery
9. ✅ Concurrent volume processing
10. ✅ Stuck volume detection and retry

## Architecture

### 1. Product Generation State Tracking

The `SQLiteStateTracker` has a new `product_generation` table for tracking product generation status.

**Database Schema**:
```sql
product_generation:
  - id (primary key)
  - volume_id (foreign key to volume_processing)
  - product_type ('image', 'geotiff', etc.)
  - status ('pending', 'processing', 'completed', 'failed')
  - generated_at (timestamp when product was generated)
  - error_message (detailed error message if failed)
  - error_type (short error type for categorization)
  - created_at, updated_at
  - UNIQUE(volume_id, product_type)
```

**Key Methods**:
- `register_product_generation()`: Register a product generation task
- `mark_product_status()`: Update product generation status with error tracking
- `get_volumes_for_product_generation()`: Get volumes ready for product generation
- `get_products_by_status()`: Query products by status and type
- `reset_stuck_product_generations()`: Reset stuck products for retry

### 2. Product Generation Daemon

The `ProductGenerationDaemon` class monitors processed NetCDF files and generates products.

**Workflow**:
```
1. Poll volume_processing table for volumes with:
   - status='completed' (NetCDF file exists)
   - No product_generation entry OR product status='pending' or 'failed'
2. For each volume:
   - Register in product_generation table
   - Mark as "processing"
   - Read NetCDF file (error tracked as "Reading volume")
   - Standardize fields (error tracked as "Standardizing fields")  
   - Generate COLMAX field if enabled
   - Generate PNG plots for all fields
   - Mark as "completed" or "failed" with error_type
3. Sleep for poll_interval
4. Repeat
```

**Error Tracking**:
- Each error has a short `error_type` (e.g., "RuntimeError", "FILE_NOT_FOUND")
- Detailed `error_message` stored for debugging
- Database updated at each processing stage

## Usage Examples

### Example 1: Basic Product Generation Daemon

Generate PNG visualizations for processed NetCDF files:

```python
import asyncio
from pathlib import Path
from radarlib.io.ftp import ProductGenerationDaemon, ProductGenerationDaemonConfig

# Define volume types (must match other daemons config)
volume_types = {
    "0315": {
        "01": ["DBZH", "DBZV", "ZDR", "RHOHV", "PHIDP", "KDP"],
        "02": ["VRAD", "WRAD"],
    },
}

config = ProductGenerationDaemonConfig(
    local_netcdf_dir=Path("./netcdf"),
    local_product_dir=Path("./products"),
    state_db=Path("./state.db"),
    volume_types=volume_types,
    radar_name="RMA1",
    poll_interval=30,  # Check every 30 seconds
    max_concurrent_processing=2,  # Process 2 volumes at once
    product_type="image",  # Generate PNG images
    add_colmax=True,  # Generate COLMAX field
)

daemon = ProductGenerationDaemon(config)
await daemon.run()
```

### Example 2: Complete Pipeline with All Three Daemons

Run download, processing, and product generation together:

```python
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from radarlib.io.ftp import (
    ContinuousDaemon,
    ContinuousDaemonConfig,
    ProcessingDaemon,
    ProcessingDaemonConfig,
    ProductGenerationDaemon,
    ProductGenerationDaemonConfig,
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

# Product generation daemon config
product_config = ProductGenerationDaemonConfig(
    local_netcdf_dir=Path("./netcdf"),
    local_product_dir=Path("./products"),
    state_db=Path("./state.db"),
    volume_types=volume_types,
    radar_name="RMA1",
    product_type="image",
    add_colmax=True,
)

async def run_pipeline():
    """Run all three daemons concurrently."""
    download_daemon = ContinuousDaemon(download_config)
    processing_daemon = ProcessingDaemon(processing_config)
    product_daemon = ProductGenerationDaemon(product_config)
    
    await asyncio.gather(
        download_daemon.run_service(),
        processing_daemon.run(),
        product_daemon.run(),
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
    product_poll_interval=30,
    enable_download_daemon=True,
    enable_processing_daemon=True,
    enable_product_daemon=True,
    product_type="image",  # or "geotiff" in future
    add_colmax=True,
)

manager = DaemonManager(manager_config)
await manager.start()
```

### Example 4: Check Product Generation Status

Query the database to see product generation progress:

```python
from pathlib import Path
from radarlib.io.ftp import SQLiteStateTracker

tracker = SQLiteStateTracker(Path("./state.db"))

# Get products by status
pending = tracker.get_products_by_status("pending", "image")
processing = tracker.get_products_by_status("processing", "image")
completed = tracker.get_products_by_status("completed", "image")
failed = tracker.get_products_by_status("failed", "image")

print(f"Pending: {len(pending)}")
print(f"Processing: {len(processing)}")
print(f"Completed: {len(completed)}")
print(f"Failed: {len(failed)}")

# Get volumes ready for product generation
ready = tracker.get_volumes_for_product_generation("image")
print(f"Ready to process: {len(ready)}")

# Check failed products
if failed:
    print("\nFailed products:")
    for prod in failed:
        print(f"  Volume: {prod['volume_id']}")
        print(f"  Error type: {prod['error_type']}")
        print(f"  Error message: {prod['error_message']}")

tracker.close()
```

## Product Generation Process

The daemon implements the full `process_volume` logic inline with proper error tracking:

1. **Read NetCDF file**: Loads the radar volume using PyART (error_type: "RuntimeError")
2. **Standardize fields**: Ensures field names are consistent (error_type: "RuntimeError")
3. **Determine reflectivity fields**: Identifies horizontal and vertical reflectivity
4. **Generate COLMAX** (if enabled): Creates column maximum field with filtering
5. **Plot unfiltered fields**: Generates PNG plots for all configured fields
6. **Plot filtered fields**: Applies quality control filters and generates filtered PNGs
7. **Database updates**: Marks success or failure with appropriate error_type

### Output Directory Structure

```
products/
├── RMA1/
│   ├── 2025/
│   │   ├── 11/
│   │   │   ├── 24/
│   │   │   │   ├── RMA1_20251124T120000Z_DBZH_00.png
│   │   │   │   ├── RMA1_20251124T120000Z_DBZV_00.png
│   │   │   │   ├── RMA1_20251124T120000Z_colmax_00.png
│   │   │   │   └── ...
```

## Error Tracking

The daemon tracks errors with two fields:

- **error_type**: Short identifier for categorization
  - `NO_NETCDF_PATH`: Missing NetCDF path in database
  - `FILE_NOT_FOUND`: NetCDF file doesn't exist
  - `RuntimeError`: Processing failures (reading, standardizing, plotting)
  - Python exception class names for other errors

- **error_message**: Detailed error message (up to 500 chars)
  - Full exception message for debugging
  - Helps understand what went wrong

## Complete and Incomplete Volumes

The daemon processes **both complete and incomplete volumes**:

- **Complete volumes**: Have all expected fields for their volume type
- **Incomplete volumes**: Missing some expected fields but NetCDF was generated

This allows visualization even when some radar data is missing or corrupted.

## Performance

- **Concurrent Processing**: Multiple volumes processed simultaneously (configurable)
- **Async Operations**: Non-blocking I/O for monitoring
- **Executor Threading**: Product generation runs in separate threads to avoid blocking
- **Stuck Volume Detection**: Automatic reset of volumes stuck in processing

## Logging

The daemon logs extensively:

- Volume detection ready for product generation
- Processing start/completion with volume completeness status
- Errors with full stack traces
- Statistics updates

**Log Levels**:
- `INFO`: Volume detection, product generation status
- `DEBUG`: File reading, COLMAX generation, detailed progress
- `ERROR`: Generation failures with tracebacks
- `WARNING`: Stuck volumes, unexpected conditions

## Integration with Other Daemons

The product generation daemon integrates seamlessly with existing daemons:

1. **Shared Database**: All daemons use the same `state.db` SQLite file
2. **Shared Config**: All use the same `volume_types` configuration
3. **Independent Operation**: Can run separately or together
4. **Data Flow**: Download → BUFR → Processing → NetCDF → Product Generation → Products

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

### ProductGenerationDaemonConfig

```python
@dataclass
class ProductGenerationDaemonConfig:
    local_netcdf_dir: Path              # Directory with NetCDF files
    local_product_dir: Path             # Directory for product output
    state_db: Path                      # SQLite database (shared)
    volume_types: Dict[str, Dict]       # Expected field types per volume
    radar_name: str                     # Radar to process (e.g., "RMA1")
    poll_interval: int = 30             # Seconds between checks
    max_concurrent_processing: int = 2  # Max simultaneous volumes
    product_type: str = "image"        # Product type ('image', 'geotiff', etc.)
    add_colmax: bool = True            # Generate COLMAX field
    stuck_volume_timeout_minutes: int = 60  # Timeout for stuck volumes
```

## Future Extensions

The architecture supports easy addition of new product types:

### Example: GeoTIFF Generation

```python
config = ProductGenerationDaemonConfig(
    ...
    product_type="geotiff",  # Different product type
    add_colmax=False,        # GeoTIFF might not need COLMAX
)
```

The daemon will:
- Track in same `product_generation` table with `product_type='geotiff'`
- Allow parallel generation of both images and GeoTIFF
- Maintain separate status for each product type per volume

## Common Use Cases

### 1. Real-time Visualization
Run all three daemons continuously for real-time pipeline from FTP to visualization.

### 2. Batch Processing
Run processing daemon to generate NetCDF files, then run product daemon to create visualizations.

### 3. Reprocessing
Reset product status in database and rerun product daemon:
```python
tracker.mark_product_status(volume_id, "image", "pending")
```

### 4. Multiple Product Types
Run multiple daemons with different `product_type` values to generate both PNG and GeoTIFF from same NetCDF.

## Troubleshooting

### Volume Not Being Processed
- Check `status` is 'completed' in volume_processing table
- Verify product_generation entry doesn't exist or has status 'pending'/'failed'
- Check NetCDF file exists at `netcdf_path`

### Product Generation Fails
- Check `error_type` and `error_message` in product_generation table
- Common error_types:
  - `FILE_NOT_FOUND`: NetCDF file missing
  - `RuntimeError`: Processing error - check error_message
- Verify NetCDF file is valid and readable
- Check output directory permissions and disk space
- Review daemon logs for detailed error info

### No Products Generated
- Verify daemon is running
- Check `volume_types` configuration matches processed volumes
- Ensure output directory is writable
- Check for failed products in database

## See Also

- [PROCESSING_DAEMON.md](PROCESSING_DAEMON.md) - NetCDF processing daemon documentation
- [examples/product_daemon_example.py](examples/product_daemon_example.py) - Usage examples
- [src/radarlib/io/pyart/vol_process.py](src/radarlib/io/pyart/vol_process.py) - Original PNG generation logic
