# radarlib.io.bufr.pyart_writer — BUFR to Py-ART conversion (English)

This module provides utilities to convert BUFR-decoded radar data (volume dicts) into
Py-ART `Radar` objects and optionally write them to CFRadial NetCDF files.

## Purpose

After decoding one or more BUFR files using `radarlib.io.bufr.bufr`, you get a list of
volume dicts with fields (reflectivity, velocity, etc.) and metadata. This module converts
such dicts into a single Py-ART `Radar` object that can be used for further analysis,
visualization, or saved to a standard NetCDF format.

## Main functions

### `bufr_fields_to_pyart_radar(fields, *, include_scan_metadata=False, ...)`

Convert a list of BUFR-decoded field dicts into a Py-ART `Radar` object.

**Inputs:**
- `fields` (List[dict]): Each element must contain `'data'` (ndarray of shape nrays × ngates) and `'info'` dict with BUFR metadata.
- `include_scan_metadata` (bool): Load XML scan strategy files if available (RMA radars only).
- `root_scan_config_files` (Optional[Path]): Path to directory containing `.xml` scan config files.
- `config` (Optional[Dict]): Custom config dict (e.g., radar coordinates).
- `debug` (bool): Enable debug logging.

**Outputs:**
- A `pyart.core.Radar` object with all fields aligned to a common range grid, elevations, azimuths, metadata, and time coordinates.

**Behavior:**
- Automatically selects the "reference field" — the one with the farthest range coverage.
- Aligns all other fields to the reference grid (zero-pads with NaN if fewer gates).
- Builds per-ray time coordinates using sweep start/end times.
- Populates instrument parameters (PRT, pulse width, antenna gain) for RMA radars if scan metadata is available.

### `bufr_paths_to_pyart(bufr_paths, *, root_resources=None, ..., save_path=None)`

High-level wrapper: decode one or more BUFR files and convert them to Py-ART objects.

**Inputs:**
- `bufr_paths` (List[str]): List of BUFR file paths.
- `root_resources` (Optional[str]): Path to BUFR resources (libdecbufr.so, bufr_tables).
- `save_path` (Optional[Path]): If provided, save each radar as a CFRadial NetCDF file in this directory.
- Other args: same as `bufr_fields_to_pyart_radar`.

**Outputs:**
- List of tuples `(bufr_path, radar_object)` for each successfully decoded file.

### `save_radar_to_cfradial(radar, out_file, format="NETCDF4")`

Save a Py-ART `Radar` object to a CFRadial NetCDF file.

**Inputs:**
- `radar`: Py-ART Radar object.
- `out_file` (Path): Output file path (must end in `.nc`).
- `format` (str): NetCDF format ('NETCDF4', 'NETCDF3_CLASSIC', etc.).

**Outputs:**
- Returns the `out_file` Path on success; raises exception on failure.

## Usage examples

### Single BUFR file to Py-ART

```python
from radarlib.io.bufr.bufr import bufr_to_dict
from radarlib.io.bufr.pyart_writer import bufr_fields_to_pyart_radar

bufr_path = "tests/data/bufr/AR5_1000_1_DBZH_20240101T000746Z.BUFR"
vol = bufr_to_dict(bufr_path, root_resources="./bufr_resources")

radar = bufr_fields_to_pyart_radar([vol])
print(radar)  # <pyart.core.Radar object>
```

### Multiple BUFR files with save to NetCDF

```python
from radarlib.io.bufr.pyart_writer import bufr_paths_to_pyart
from pathlib import Path

bufr_files = ["file1.BUFR", "file2.BUFR", "file3.BUFR"]
results = bufr_paths_to_pyart(
    bufr_files,
    root_resources="./bufr_resources",
    save_path=Path("./output_netcdf")
)

for bufr_path, radar in results:
    print(f"Converted {bufr_path} → {radar.nrays} rays, {radar.ngates} gates")
```

## Data alignment and reference field selection

When multiple products (DBZH, VRAD, etc.) have different numbers of gates or offsets:
- The module selects the product with the farthest range (largest `gate_offset + gate_size*ngates`).
- All other products are interpolated/padded to match this reference grid using masked arrays.
- NaN is used for missing/padded values.

This ensures a rectangular data matrix suitable for standard tools.

## Testing

- **Unit tests** (`tests/unit/test_pyart_writer.py`): Test reference field selection and alignment logic with synthetic data.
- **Integration tests** (`tests/integration/test_end_to_end_bufr_to_pyart.py`): Full end-to-end test using a real BUFR file and C library (skipped if resources unavailable).

Run tests:
```bash
pytest tests/unit/test_pyart_writer.py
pytest tests/integration/test_end_to_end_bufr_to_pyart.py
```

## Dependencies

- `arm-pyart` — Py-ART radar toolkit (includes `pyart` module).
- `netCDF4` — NetCDF file I/O (required by Py-ART for CFRadial writing).
- `numpy`, `pandas` — Array and dataframe operations.

## Notes

- The current implementation uses `pyart.testing.make_empty_ppi_radar()` to create the Radar object, which assumes a PPI (plan position indicator) scan type. For RHI or other scan types, further customization may be needed.
- Metadata from XML scan config files is optional and only loaded when explicitly requested for RMA radars.
- Non-serializable metadata (dict objects) are removed before writing to NetCDF to avoid type errors in the netCDF4 library.
