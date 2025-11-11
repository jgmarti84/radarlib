# radarlib.io.bufr.bufr — BUFR decoding utilities (English)

This module provides functions to decode legacy BUFR radar files using a C
shared library (`libdecbufr.so`) together with helper utilities to parse,
decompress and assemble radar sweep data into numpy arrays and a structured
`info` dictionary suitable for downstream processing.

Contents
- Purpose and high-level flow
- Important types and exceptions
- Public functions (signature, description, return values)
- Usage examples
- Errors, edge cases, and testing notes
- Deployment notes (C library, resources)

Purpose and high-level flow
---------------------------
`radarlib.io.bufr.bufr` acts as a thin Python glue layer around a C decoder
for BUFR files. The typical processing flow is:

1. Load `libdecbufr.so` (a small C library included under the project resources).
2. Call C functions to read volume size, raw integer block and fixed-angle elevations.
3. Parse the integer buffer into per-sweep headers and compressed byte chunks.
4. Decompress the per-sweep bytes with `zlib` and reshape into 2-D arrays
   (rays × gates).
5. Uniformize gate counts across sweeps, vertically concatenate sweeps into
   a single 2-D volume, and build a metadata `info` dict.

Important types and exceptions
------------------------------
- `SweepConsistencyException` — raised when a sweep has an invalid number of gates
  (used to discard bad sweeps).
- `point_t`, `meta_t` — ctypes.Structure definitions used when calling the
  C library. These are internal and you normally won't instantiate them.

Public functions
----------------
Below are the main helpers and their contracts.

- `decbufr_library_context(root_resources: Optional[str] = None) -> CDLL context`
  - Context manager that yields a loaded `ctypes.CDLL` object for the C library.
  - Uses `config.BUFR_RESOURCES_PATH` when `root_resources` is not provided.

- `bufr_name_metadata(bufr_filename: str) -> dict`
  - Parse standard BUFR filename pattern `RADAR_STRATEGY_NVOL_TYPE_TIMESTAMP.BUFR`.
  - Returns keys: `radar_name`, `estrategia_nombre`, `estrategia_nvol`,
    `tipo_producto`, `filename`.
  - Raises `ValueError` if filename pattern doesn't match.

- `load_decbufr_library(root_resources: str) -> CDLL`
  - Loads `dynamic_library/libdecbufr.so` from the given resources path.

- `get_metadata(lib: CDLL, bufr_path: str, root_resources: Optional[str]) -> dict`
  - Uses the C function `get_meta_data` and returns a dict with
    `year, month, day, hour, min, lat, lon, radar_height`.

- `get_elevations(lib: CDLL, bufr_path: str, max_elev: int = 30, ...) -> np.ndarray`
  - Returns a 1-D float array of fixed-angle elevations.

- `get_raw_volume(lib: CDLL, bufr_path: str, size: int, ...) -> np.ndarray`
  - Returns the raw integer buffer (1-D numpy array) created by the C decoder.

- `get_size_data(lib: CDLL, bufr_path: str, ...) -> int`
  - Calls the C helper to get the expected size (number of ints) of the raw
    buffer.

- `parse_sweeps(vol: np.ndarray, nsweeps: int, elevs: np.ndarray) -> list[dict]`
  - Parses the integer buffer into a list of per-sweep dicts. Each sweep
    contains header fields and a `compress_data` bytearray with the
    concatenated compressed chunks.

- `decompress_sweep(sweep: dict) -> np.ndarray`
  - Decompresses `sweep['compress_data']` using `zlib.decompress`, interprets
    the result as `float64`, masks a sentinel, and reshapes to `(nrays, ngates)`.
  - Raises `SweepConsistencyException` when `ngates` is implausibly large and
    `ValueError` when decompressed length doesn't match `nrays * ngates`.

- `uniformize_sweeps(sweeps: list[dict]) -> list[dict]`
  - Pads sweeps with fewer gates with `NaN` columns so all sweeps share the
    same `ngates` (the maximum among sweeps).

- `assemble_volume(sweeps: list[dict]) -> np.ndarray`
  - Vertically stacks per-sweep arrays into a single `(total_rays, ngates)`
    `numpy.ndarray`.

- `validate_sweeps_df(sweeps_df: pd.DataFrame) -> pd.DataFrame`
  - Performs basic consistency checks using a DataFrame (same number of rays,
    same gate size, acceptable gate offsets). Raises `AssertionError` on
    inconsistencies.

- `build_metadata(filename: str, info: dict) -> dict`
  - Creates a standardized metadata dictionary used by the pipeline.

- `build_info_dict(meta_vol: dict, meta_sweeps: list[dict]) -> dict`
  - Builds the full `info` structure used by consumers. Internally converts
    per-sweep dicts into a DataFrame, validates them, and attaches metadata.

- `dec_bufr_file(bufr_filename: str, root_resources: Optional[str] = None,
                  logger_name: Optional[str] = None, parallel: bool = True)
  -> (meta_vol, sweeps, vol_data, run_log)`
  - High-level function that executes the full decoding flow: loads the C
    library, reads size/elevations/raw buffer, parses sweeps, decompresses
    (optionally in parallel using `ThreadPoolExecutor`), uniformizes and
    assembles the volume, and returns metadata, the sweep list (each with
    a `data` ndarray), the combined volume ndarray, and a `run_log` list.
  - `run_log` entries use numeric levels (e.g. 2 for warnings, 3 for errors).
  - Raises `ValueError` when an unrecoverable error occurs.

- `bufr_to_dict(bufr_filename: str, root_resources: Optional[str] = None,
               logger_name: str | None = None, legacy: bool = False) -> Optional[dict]`
  - Convenience wrapper that calls `dec_bufr_file` with retry/backoff and
    returns a dictionary `{'data': ndarray, 'info': dict}` on success or
    `None` on failure (after logging). When `legacy=True`, it flattens
    sweep lists into the returned `info` dict for compatibility.

Usage examples
--------------
Basic usage with default resource path (requires `config.BUFR_RESOURCES_PATH` to be set
or the library present at that location):

```python
from radarlib.io.bufr.bufr import bufr_to_dict

result = bufr_to_dict('tests/data/bufr/AR5_1000_1_DBZH_20240101T000746Z.BUFR')
if result is None:
    print('processing failed')
else:
    data = result['data']        # numpy ndarray (rays, gates)
    info = result['info']        # metadata dict + sweeps info
```

Parallel decompression
-----------------------
Call `dec_bufr_file(..., parallel=True)` to decompress sweeps in parallel.
Make sure to monitor memory usage because decompression is done per-sweep in
memory.

Errors and edge cases
---------------------
- The code relies on a working `libdecbufr.so`. If that library is missing or
  invalid, `cdll.LoadLibrary` will raise an `OSError`.
- Legacy resource files under `bufr_resources` may contain Python2 code or
  large data files. Consider excluding `src/radarlib/io/bufr/bufr_resources`
  from formatters/linters (Black, Flake8) and pre-commit hooks.
- `decompress_sweep` will raise `ValueError` if the decompressed payload size
  does not match the expected `nrays * ngates`.
- `SweepConsistencyException` is raised for implausible `ngates` values and
  used to discard bad sweeps.

Testing notes
-------------
- Unit tests in the repository use monkeypatching to simulate C library
  behavior for `dec_bufr_file` and `get_*` helpers. Integration tests expect a
  valid `.BUFR` test file and a working `libdecbufr.so` available in
  `tests/data` or the configured resources path.

Deployment and packaging notes
-----------------------------
- Ensure `dynamic_library/libdecbufr.so` and `bufr_tables` are included in
  package data when building wheels or installing the package. The module
  expects `config.BUFR_RESOURCES_PATH` to point to the directory containing
  these resources.
- For reproducible CI runs, either install `isort` in the runner or use the
  system/local isort hook in `.pre-commit-config.yaml` to avoid remote mirror
  checkout errors.

Contact and further improvements
--------------------------------
- Consider adding defensive input validation (Path objects, file existence
  checks) and better-typed return values (TypedDicts) to make the API safer
  for downstream code.
