# -*- coding: utf-8 -*-
"""PNG generation daemon for monitoring and generating PNG plots from processed NetCDF volumes."""

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from radarlib.io.pyart.vol_process import process_volume

from .sqlite_state_tracker import SQLiteStateTracker

logger = logging.getLogger(__name__)


@dataclass
class PNGGenerationDaemonConfig:
    """
    Configuration for PNG generation daemon service.

    Attributes:
        local_netcdf_dir: Directory containing processed NetCDF files
        local_png_dir: Directory to save PNG output files
        state_db: Path to SQLite database for tracking state
        volume_types: Dict mapping volume codes to valid volume numbers and field types.
                     Format: {'0315': {'01': ['DBZH', 'DBZV'], '02': ['VRAD']}}
        radar_name: Radar name to process (e.g., "RMA1")
        poll_interval: Seconds between checks for new volumes to process
        max_concurrent_processing: Maximum simultaneous PNG generation tasks
        add_colmax: Whether to generate COLMAX field
        stuck_volume_timeout_minutes: Minutes to wait before resetting a stuck volume from
                                      'processing' status back to 'pending' for retry
    """

    local_netcdf_dir: Path
    local_png_dir: Path
    state_db: Path
    volume_types: Dict[str, Dict[str, List[str]]]
    radar_name: str
    poll_interval: int = 30
    max_concurrent_processing: int = 2
    add_colmax: bool = True
    stuck_volume_timeout_minutes: int = 60


class PNGGenerationDaemon:
    """
    Daemon for monitoring and generating PNG plots from processed NetCDF volumes.

    This daemon monitors the volume_processing table in the SQLite database,
    detects volumes with status='completed' (NetCDF files generated) and
    png_status='pending', reads the NetCDF file, generates PNG plots for all
    fields, and generates the COLMAX field.

    The daemon mimics the functionality of the process_volume function from
    vol_process.py, generating field PNGs and COLMAX.

    Example:
        >>> from pathlib import Path
        >>> config = PNGGenerationDaemonConfig(
        ...     local_netcdf_dir=Path("./netcdf"),
        ...     local_png_dir=Path("./png"),
        ...     state_db=Path("./state.db"),
        ...     volume_types={'0315': {'01': ['DBZH', 'DBZV'], '02': ['VRAD']}},
        ...     radar_name="RMA1"
        ... )
        >>> daemon = PNGGenerationDaemon(config)
        >>> asyncio.run(daemon.run())
    """

    def __init__(self, config: PNGGenerationDaemonConfig):
        """
        Initialize the PNG generation daemon.

        Args:
            config: Daemon configuration
        """
        self.config = config
        self.state_tracker = SQLiteStateTracker(config.state_db)
        self._running = False
        self._processing_semaphore: Optional[asyncio.Semaphore] = None

        # Ensure output directory exists
        self.config.local_png_dir.mkdir(parents=True, exist_ok=True)

        # Statistics
        self._stats = {
            "volumes_processed": 0,
            "volumes_failed": 0,
        }

    async def run(self) -> None:
        """
        Run the daemon to monitor and generate PNGs for processed volumes.

        Continuously checks for volumes ready for PNG generation and processes them.
        """
        self._running = True
        self._processing_semaphore = asyncio.Semaphore(self.config.max_concurrent_processing)

        logger.info(f"Starting PNG generation daemon for radar '{self.config.radar_name}'")
        logger.info(f"Monitoring NetCDF files in '{self.config.local_netcdf_dir}'")
        logger.info(f"Saving PNG files to '{self.config.local_png_dir}'")
        logger.info(
            f"Configuration: poll_interval={self.config.poll_interval}s, "
            f"max_concurrent={self.config.max_concurrent_processing}, "
            f"stuck_timeout={self.config.stuck_volume_timeout_minutes}min, "
            f"add_colmax={self.config.add_colmax}"
        )

        try:
            while self._running:
                try:
                    # Check for and reset stuck volumes
                    await self._check_and_reset_stuck_volumes()

                    # Process volumes ready for PNG generation
                    await self._process_volumes_for_png()

                    # Wait before next check
                    await asyncio.sleep(self.config.poll_interval)

                except Exception as e:
                    logger.error(f"Error during PNG generation cycle: {e}", exc_info=True)
                    await asyncio.sleep(self.config.poll_interval)

        except asyncio.CancelledError:
            logger.info("PNG daemon cancelled, shutting down...")
        except KeyboardInterrupt:
            logger.info("PNG daemon interrupted, shutting down...")
        finally:
            self._running = False
            # Log final statistics
            logger.info(
                f"PNG daemon shutting down. Statistics: "
                f"processed={self._stats['volumes_processed']}, "
                f"failed={self._stats['volumes_failed']}"
            )
            self.state_tracker.close()
            logger.info(f"PNG daemon for '{self.config.radar_name}' stopped")

    def stop(self) -> None:
        """Stop the daemon gracefully."""
        self._running = False
        logger.info("PNG daemon stop requested")

    async def _check_and_reset_stuck_volumes(self) -> None:
        """
        Check for volumes stuck in 'processing' status and reset them to 'pending'.

        Volumes that have been in 'processing' status for longer than the configured
        timeout will be reset to 'pending' and logged for retry.
        """
        try:
            num_reset = self.state_tracker.reset_stuck_png_volumes(self.config.stuck_volume_timeout_minutes)
            if num_reset > 0:
                logger.warning(
                    f"Reset {num_reset} stuck PNG volume(s) from 'processing' to 'pending' "
                    f"(timeout: {self.config.stuck_volume_timeout_minutes} minutes)"
                )
        except Exception as e:
            logger.error(f"Error checking for stuck PNG volumes: {e}", exc_info=True)

    async def _process_volumes_for_png(self) -> None:
        """
        Process all volumes that are ready for PNG generation.

        Gets volumes with status='completed' and png_status='pending' or 'failed',
        and generates PNG plots for them.
        """
        # Get all volumes ready for PNG generation
        volumes = self.state_tracker.get_volumes_for_png_generation()

        if not volumes:
            logger.debug(f"No volumes ready for PNG generation for {self.config.radar_name}")
            return

        logger.info(f"Found {len(volumes)} volume(s) ready for PNG generation")

        # Process volumes concurrently
        tasks = []
        for volume_info in volumes:
            task = self._generate_png_async(volume_info)
            tasks.append(task)

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            # Count successes and failures
            num_success = sum(1 for r in results if r is True)
            num_failed = sum(1 for r in results if r is False or isinstance(r, Exception))
            if num_failed > 0:
                logger.warning(f"PNG generation complete: {num_success} succeeded, {num_failed} failed")

    async def _generate_png_async(self, volume_info: Dict) -> bool:
        """
        Generate PNG plots for a single volume asynchronously.

        Args:
            volume_info: Dictionary with volume information from database

        Returns:
            True if successful, False otherwise
        """
        if self._processing_semaphore is None:
            raise RuntimeError("Processing semaphore not initialized. Call run() first.")

        async with self._processing_semaphore:
            volume_id = volume_info["volume_id"]
            netcdf_path = volume_info.get("netcdf_path")
            is_complete = volume_info.get("is_complete", 0) == 1

            if not netcdf_path:
                logger.error(f"No NetCDF path found for volume {volume_id}")
                self.state_tracker.mark_volume_png_status(
                    volume_id, "failed", error_message="No NetCDF path found"
                )
                self._stats["volumes_failed"] += 1
                return False

            netcdf_file = Path(netcdf_path)
            if not netcdf_file.exists():
                logger.error(f"NetCDF file not found: {netcdf_file}")
                self.state_tracker.mark_volume_png_status(
                    volume_id, "failed", error_message=f"NetCDF file not found: {netcdf_file}"
                )
                self._stats["volumes_failed"] += 1
                return False

            completeness_str = "complete" if is_complete else "incomplete"
            logger.info(f"Generating PNGs for {completeness_str} volume {volume_id}...")

            # Mark as processing
            self.state_tracker.mark_volume_png_status(volume_id, "processing")

            try:
                # Generate PNGs using process_volume logic
                await self._generate_pngs_for_volume(netcdf_file, volume_info)

                # Mark as completed
                self.state_tracker.mark_volume_png_status(volume_id, "completed")
                logger.info(f"Successfully generated PNGs for {completeness_str} volume {volume_id}")
                self._stats["volumes_processed"] += 1
                return True

            except Exception as e:
                error_msg = f"Failed to generate PNGs for {completeness_str} volume {volume_id}: {str(e)}"
                logger.error(error_msg, exc_info=True)
                self.state_tracker.mark_volume_png_status(volume_id, "failed", error_message=error_msg)
                self._stats["volumes_failed"] += 1
                return False

    async def _generate_pngs_for_volume(self, netcdf_path: Path, volume_info: Dict) -> None:
        """
        Generate PNG plots for all fields in a volume.

        Uses the process_volume function logic from vol_process.py to generate
        field PNGs and COLMAX.

        Args:
            netcdf_path: Path to the NetCDF file
            volume_info: Volume information from database

        Raises:
            Exception if PNG generation fails
        """
        # Build volume types dict for this specific volume
        vol_types = self.config.volume_types

        # Run process_volume in executor to avoid blocking
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None, process_volume, str(netcdf_path), vol_types, self.config.add_colmax, logger.name
        )

        logger.debug(f"PNG generation completed for {netcdf_path}")

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
            "pending_volumes": len(self.state_tracker.get_volumes_by_png_status("pending")),
            "completed_volumes": len(self.state_tracker.get_volumes_by_png_status("completed")),
        }
