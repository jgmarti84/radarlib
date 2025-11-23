# -*- coding: utf-8 -*-
"""Processing daemon for monitoring and processing complete BUFR volumes."""

import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from .sqlite_state_tracker import SQLiteStateTracker

logger = logging.getLogger(__name__)


@dataclass
class ProcessingDaemonConfig:
    """
    Configuration for BUFR processing daemon service.

    Attributes:
        local_bufr_dir: Directory containing downloaded BUFR files
        local_netcdf_dir: Directory to save processed NetCDF files
        state_db: Path to SQLite database for tracking state
        volume_types: Dict mapping volume codes to valid volume numbers and field types.
                     Format: {'0315': {'01': ['DBZH', 'DBZV'], '02': ['VRAD']}}
        radar_name: Radar name to process (e.g., "RMA1")
        poll_interval: Seconds between checks for new complete volumes
        max_concurrent_processing: Maximum simultaneous volume processing tasks
        root_resources: Path to BUFR resources directory (for decoding)
        allow_incomplete: Whether to process incomplete volumes (missing some fields)
        incomplete_timeout_hours: Hours to wait before processing incomplete volumes
    """

    local_bufr_dir: Path
    local_netcdf_dir: Path
    state_db: Path
    volume_types: Dict[str, Dict[str, List[str]]]
    radar_name: str
    poll_interval: int = 30
    max_concurrent_processing: int = 2
    root_resources: Optional[Path] = None
    allow_incomplete: bool = False
    incomplete_timeout_hours: int = 24


class ProcessingDaemon:
    """
    Daemon for monitoring and processing complete BUFR volumes.

    This daemon monitors downloaded BUFR files tracked in the SQLite database,
    detects when complete volumes are available (all required field types for
    a volume code/number combination), decodes the BUFR files, creates a PyART
    Radar object, and saves it as a NetCDF file.

    Example:
        >>> from pathlib import Path
        >>> config = ProcessingDaemonConfig(
        ...     local_bufr_dir=Path("./downloads"),
        ...     local_netcdf_dir=Path("./netcdf"),
        ...     state_db=Path("./state.db"),
        ...     volume_types={'0315': {'01': ['DBZH', 'DBZV'], '02': ['VRAD']}},
        ...     radar_name="RMA1"
        ... )
        >>> daemon = ProcessingDaemon(config)
        >>> asyncio.run(daemon.run())
    """

    def __init__(self, config: ProcessingDaemonConfig):
        """
        Initialize the processing daemon.

        Args:
            config: Daemon configuration
        """
        self.config = config
        self.state_tracker = SQLiteStateTracker(config.state_db)
        self._running = False
        self._processing_semaphore: Optional[asyncio.Semaphore] = None

        # Ensure output directory exists
        self.config.local_netcdf_dir.mkdir(parents=True, exist_ok=True)

        # Statistics
        self._stats = {
            "volumes_processed": 0,
            "volumes_failed": 0,
            "incomplete_volumes_detected": 0,
        }

    async def run(self) -> None:
        """
        Run the daemon to monitor and process complete volumes.

        Continuously checks for complete volumes and processes them.
        """
        self._running = True
        self._processing_semaphore = asyncio.Semaphore(self.config.max_concurrent_processing)

        logger.info(f"Starting BUFR processing daemon for radar '{self.config.radar_name}'")
        logger.info(f"Monitoring BUFR files in '{self.config.local_bufr_dir}'")
        logger.info(f"Saving NetCDF files to '{self.config.local_netcdf_dir}'")

        try:
            while self._running:
                try:
                    # Update volume completion status
                    await self._check_volume_completeness()

                    # Process complete volumes
                    await self._process_complete_volumes()

                    # Wait before next check
                    await asyncio.sleep(self.config.poll_interval)

                except Exception as e:
                    logger.error(f"Error during processing cycle: {e}", exc_info=True)
                    await asyncio.sleep(self.config.poll_interval)

        except asyncio.CancelledError:
            logger.info("Daemon cancelled, shutting down...")
        except KeyboardInterrupt:
            logger.info("Daemon interrupted, shutting down...")
        finally:
            self._running = False
            self.state_tracker.close()

    def stop(self) -> None:
        """Stop the daemon gracefully."""
        self._running = False
        logger.info("Daemon stop requested")

    async def _check_volume_completeness(self) -> None:
        """
        Check downloaded files and update volume completion status.

        Scans the downloads table for files belonging to volumes and determines
        if each volume is complete based on the expected field types.
        """
        # Get all downloaded files for this radar
        conn = self.state_tracker._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT filename, radar_name, field_type, observation_datetime, local_path
            FROM downloads
            WHERE radar_name = ? AND status = 'completed'
            ORDER BY observation_datetime
        """,
            (self.config.radar_name,),
        )

        files = cursor.fetchall()

        # Group files by volume (radar + strategy + vol_nr + timestamp)
        volumes: Dict[str, Dict] = {}

        for row in files:
            filename = row[0]
            radar_name = row[1]
            field_type = row[2]
            observation_datetime = row[3]
            local_path = row[4]

            # Parse filename: RADAR_STRATEGY_VOLNR_FIELD_TIMESTAMP.BUFR
            try:
                parts = filename.split("_")
                if len(parts) < 4:
                    logger.debug(f"Skipping file with unexpected format: {filename}")
                    continue

                strategy = parts[1]
                vol_nr = parts[2]

                # Check if this volume type is configured
                if strategy not in self.config.volume_types:
                    continue

                if vol_nr not in self.config.volume_types[strategy]:
                    continue

                # Create volume key
                volume_id = self.state_tracker.get_volume_id(radar_name, strategy, vol_nr, observation_datetime)

                if volume_id not in volumes:
                    volumes[volume_id] = {
                        "radar_name": radar_name,
                        "strategy": strategy,
                        "vol_nr": vol_nr,
                        "observation_datetime": observation_datetime,
                        "expected_fields": self.config.volume_types[strategy][vol_nr],
                        "downloaded_fields": set(),
                        "files": {},
                    }

                volumes[volume_id]["downloaded_fields"].add(field_type)
                volumes[volume_id]["files"][field_type] = local_path

            except Exception as e:
                logger.warning(f"Error parsing filename {filename}: {e}")
                continue

        # Update volume status in database
        for volume_id, vol_info in volumes.items():
            expected = set(vol_info["expected_fields"])
            downloaded = vol_info["downloaded_fields"]
            is_complete = expected == downloaded

            # Check if volume already exists in database
            existing = self.state_tracker.get_volume_info(volume_id)

            if existing:
                # Update if status changed
                if existing["is_complete"] != (1 if is_complete else 0):
                    self.state_tracker.update_volume_fields(volume_id, list(downloaded), is_complete)
                    if is_complete:
                        logger.info(f"Volume {volume_id} is now complete")
                    else:
                        missing = expected - downloaded
                        logger.debug(f"Volume {volume_id} incomplete, missing: {missing}")
            else:
                # Register new volume
                self.state_tracker.register_volume(
                    volume_id,
                    vol_info["radar_name"],
                    vol_info["strategy"],
                    vol_info["vol_nr"],
                    vol_info["observation_datetime"],
                    vol_info["expected_fields"],
                    is_complete,
                )
                if is_complete:
                    logger.info(f"New complete volume detected: {volume_id}")
                else:
                    missing = expected - downloaded
                    logger.debug(f"New incomplete volume detected: {volume_id}, missing: {missing}")
                    self._stats["incomplete_volumes_detected"] += 1

    async def _process_complete_volumes(self) -> None:
        """
        Process all complete volumes that haven't been processed yet.
        """
        # Get complete unprocessed volumes
        complete_volumes = self.state_tracker.get_complete_unprocessed_volumes()

        if not complete_volumes:
            logger.debug("No complete volumes to process")
            return

        logger.info(f"Found {len(complete_volumes)} complete volume(s) to process")

        # Process volumes concurrently
        tasks = []
        for volume_info in complete_volumes:
            task = self._process_volume_async(volume_info)
            tasks.append(task)

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _process_volume_async(self, volume_info: Dict) -> bool:
        """
        Process a single volume asynchronously.

        Args:
            volume_info: Dictionary with volume information from database

        Returns:
            True if successful, False otherwise
        """
        async with self._processing_semaphore:
            volume_id = volume_info["volume_id"]
            radar_name = volume_info["radar_name"]
            strategy = volume_info["strategy"]
            vol_nr = volume_info["vol_nr"]
            observation_datetime = volume_info["observation_datetime"]

            logger.info(f"Processing volume {volume_id}...")

            # Mark as processing
            self.state_tracker.mark_volume_processing(volume_id, "processing")

            try:
                # Get all files for this volume
                files = self.state_tracker.get_volume_files(radar_name, strategy, vol_nr, observation_datetime)

                if not files:
                    raise ValueError(f"No files found for volume {volume_id}")

                # Get full paths to BUFR files
                bufr_paths = []
                for filename in files:
                    file_info = self.state_tracker.get_file_info(filename)
                    if file_info and file_info["local_path"]:
                        bufr_paths.append(file_info["local_path"])

                if not bufr_paths:
                    raise ValueError(f"No local paths found for volume {volume_id}")

                logger.info(f"Decoding {len(bufr_paths)} BUFR files for volume {volume_id}")

                # Process in executor to avoid blocking
                loop = asyncio.get_event_loop()
                netcdf_path = await loop.run_in_executor(
                    None, self._decode_and_save_volume, bufr_paths, volume_id, radar_name
                )

                # Mark as completed
                self.state_tracker.mark_volume_processing(volume_id, "completed", str(netcdf_path))
                logger.info(f"Successfully processed volume {volume_id} -> {netcdf_path}")
                self._stats["volumes_processed"] += 1
                return True

            except Exception as e:
                error_msg = f"Failed to process volume {volume_id}: {str(e)}"
                logger.error(error_msg, exc_info=True)
                self.state_tracker.mark_volume_processing(volume_id, "failed", error_message=error_msg)
                self._stats["volumes_failed"] += 1
                return False

    def _decode_and_save_volume(self, bufr_paths: List[str], volume_id: str, radar_name: str) -> Path:
        """
        Decode BUFR files and save as NetCDF.

        Args:
            bufr_paths: List of paths to BUFR files
            volume_id: Unique volume identifier
            radar_name: Radar name

        Returns:
            Path to saved NetCDF file

        Raises:
            Exception if decoding or saving fails
        """
        # Import here to avoid issues if pyart not available
        from radarlib.io.bufr.bufr import bufr_to_dict
        from radarlib.io.bufr.pyart_writer import bufr_fields_to_pyart_radar
        from radarlib.utils.names_utils import get_netcdf_filename_from_bufr_filename

        # Decode all BUFR files
        logger.debug(f"Decoding {len(bufr_paths)} BUFR files...")
        decoded_fields = []

        for bufr_path in bufr_paths:
            try:
                decoded = bufr_to_dict(
                    bufr_path, root_resources=str(self.config.root_resources) if self.config.root_resources else None
                )
                if decoded:
                    decoded_fields.append(decoded)
                else:
                    logger.warning(f"Failed to decode {bufr_path}")
            except Exception as e:
                logger.error(f"Error decoding {bufr_path}: {e}")
                # Continue with other files

        if not decoded_fields:
            raise ValueError(f"No BUFR files could be decoded for volume {volume_id}")

        logger.debug(f"Successfully decoded {len(decoded_fields)} fields")

        # Create PyART Radar object
        logger.debug("Creating PyART Radar object...")
        radar = bufr_fields_to_pyart_radar(decoded_fields)

        if radar is None:
            raise ValueError(f"Failed to create Radar object for volume {volume_id}")

        # Generate NetCDF filename from first BUFR file
        first_bufr_name = Path(bufr_paths[0]).name
        netcdf_filename = get_netcdf_filename_from_bufr_filename(first_bufr_name)
        netcdf_path = self.config.local_netcdf_dir / netcdf_filename

        # Save to NetCDF
        logger.debug(f"Saving to NetCDF: {netcdf_path}")
        import pyart

        pyart.io.write_cfradial(str(netcdf_path), radar)

        logger.info(f"Successfully saved NetCDF file: {netcdf_path}")
        return netcdf_path

    def get_stats(self) -> Dict:
        """
        Get daemon statistics.

        Returns:
            Dictionary with daemon stats
        """
        return {
            "running": self._running,
            "volumes_processed": self._stats["volumes_processed"],
            "volumes_failed": self._stats["volumes_failed"],
            "incomplete_volumes_detected": self._stats["incomplete_volumes_detected"],
            "pending_volumes": len(self.state_tracker.get_volumes_by_status("pending")),
            "complete_unprocessed": len(self.state_tracker.get_complete_unprocessed_volumes()),
        }
