"""
Example: End-to-end PNG generation from RMA5 BUFR files using legacy pipeline.

This example demonstrates:
1. Decoding BUFR files with bufr_to_dict
2. Creating PyART Radar objects with the legacy converter
3. Generating PPI visualizations as PNG files
"""

import logging
from collections import defaultdict
from pathlib import Path

# Import from the refactored API
from radarlib.io.bufr.bufr import bufr_to_dict
from radarlib.io.bufr.legacy.pyart_legacy import bufr_to_pyart_legacy
from radarlib.io.bufr.pyart_writer import save_radar_to_cfradial

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def example_netcdf_generation():
    """ """
    logger.info("=" * 80)
    logger.info("Example 1: Radar NetCDF Generation (Multiple BUFR Files)")
    logger.info("=" * 80)

    # Path to BUFR files
    bufr_dir = Path("tests/data/bufr/RMA11")
    bufr_files = sorted(list(bufr_dir.glob("*.BUFR")))

    if not bufr_files:
        logger.error(f"No BUFR files found in {bufr_dir}")
        return

    groups = defaultdict(list)
    for p in bufr_files:
        stem = p.stem  # e.g., "RMA11_0315_01_DBZH_20251020T151109Z"
        timestamp = stem.split("_")[-1]
        groups[timestamp].append(p)

    # for ts, items in groups.items():
    #     print(ts, items)
    bufr_files = groups[list(groups.keys())[0]]
    # Decode first two BUFR files (or all available if less than 2)
    # num_files = min(2, len(bufr_files))
    bufr_dicts = []

    # for i in range(num_files):
    for bufr_path in bufr_files:
        bufr_file = str(bufr_path)
        logger.info(f"Decoding file: {Path(bufr_file).name}")

        # Decode BUFR file using bufr_to_dict with legacy=True to get format
        # compatible with bufr_to_pyart_legacy
        bufr_dict = bufr_to_dict(bufr_file, logger_name="example", legacy=True)

        if bufr_dict is None:
            logger.warning(f"  Failed to decode BUFR file: {bufr_file}")
            continue

        logger.info(f"  ✓ Decoded successfully. Data shape: {bufr_dict['data'].shape}")
        bufr_dicts.append(bufr_dict)

    if not bufr_dicts:
        logger.error("No BUFR files were successfully decoded")
        return

    logger.info(f"\n✓ Successfully decoded {len(bufr_dicts)} BUFR files")

    # Create radar object using legacy converter with list of field dicts
    logger.info("Creating PyART Radar object from decoded fields...")
    radar = bufr_to_pyart_legacy(fields=bufr_dicts, logger_name="example")

    logger.info(f"✓ Radar created with fields: {list(radar.fields.keys())}")

    # Output directory
    output_dir = Path("outputs/example_netcdfs/")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_filename = output_dir / radar.metadata.get("filename", "radar_data.nc")
    save_radar_to_cfradial(radar, output_filename)

    logger.info(f"✓ NETCDF saved to: {output_filename}")
    logger.info("")


if __name__ == "__main__":
    logger.info("\n")
    logger.info("╔" + "=" * 78 + "╗")
    logger.info("║" + " " * 78 + "║")
    logger.info("║" + "  PyART Radar PNG Generation Examples".center(78) + "║")
    logger.info("║" + "  Using Legacy BUFR Pipeline".center(78) + "║")
    logger.info("║" + " " * 78 + "║")
    logger.info("╚" + "=" * 78 + "╝")
    logger.info("")

    # Run examples
    try:
        example_netcdf_generation()

        logger.info("╔" + "=" * 78 + "╗")
        logger.info("║" + "  Example completed successfully! ✓".center(78) + "║")
        logger.info("╚" + "=" * 78 + "╝")

    except Exception as e:
        logger.error(f"Error during examples: {e}", exc_info=True)
