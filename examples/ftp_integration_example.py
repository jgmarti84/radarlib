"""
Complete integration example: FTP daemon with BUFR processing.

This example demonstrates a complete workflow:
1. Download BUFR files from FTP server using the daemon
2. Process files using radarlib's BUFR decoder
3. Generate radar products

This is a template for a production-ready radar data processing pipeline.
"""

import asyncio
import logging
from pathlib import Path

from radarlib.io.ftp import FTPDaemon, FTPDaemonConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("radar_ftp_daemon.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class RadarDataProcessor:
    """
    Process downloaded BUFR files.
    
    This class demonstrates how to integrate FTP downloads with BUFR processing.
    """
    
    def __init__(self, output_dir: str = "/data/radar/products"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.processed_count = 0
        
    def process_bufr_file(self, local_path: str):
        """
        Process a downloaded BUFR file.
        
        Args:
            local_path: Path to the downloaded BUFR file
        """
        try:
            file_path = Path(local_path)
            logger.info(f"Processing BUFR file: {file_path.name}")
            
            # Example processing workflow:
            # 1. Decode BUFR file
            # from radarlib.io.bufr import bufr_to_dict, bufr_to_pyart
            # bufr_dict = bufr_to_dict(str(file_path))
            
            # 2. Convert to PyART radar object
            # radar = bufr_to_pyart([bufr_dict])
            
            # 3. Generate products (PNG, GeoTIFF, etc.)
            # from radarlib.io.pyart import plot_and_save_ppi
            # output_file = self.output_dir / f"{file_path.stem}.png"
            # plot_and_save_ppi(radar, 'reflectivity', str(output_file))
            
            # For this example, we'll just log success
            self.processed_count += 1
            logger.info(f"✓ Successfully processed {file_path.name} (total: {self.processed_count})")
            
        except Exception as e:
            logger.error(f"Error processing {local_path}: {e}", exc_info=True)


async def run_production_daemon():
    """
    Run the FTP daemon in production mode with complete error handling.
    """
    logger.info("=" * 80)
    logger.info("Starting Radar FTP Daemon - Production Mode")
    logger.info("=" * 80)
    
    # Create processor
    processor = RadarDataProcessor(output_dir="/tmp/radar_products")
    
    # Configure daemon
    config = FTPDaemonConfig(
        host="ftp.example.com",
        username="your_username",
        password="your_password",  # In production, use environment variables or secrets manager
        remote_path="/radar/data",
        local_dir="/tmp/radar_incoming",
        port=21,
        file_pattern="*.BUFR",
        poll_interval=30,  # Check every 30 seconds
        max_concurrent_downloads=10,  # Download up to 10 files at once
        recursive=False,
    )
    
    # Create daemon with processor callback
    daemon = FTPDaemon(
        config, 
        on_file_downloaded=processor.process_bufr_file,
        logger=logger
    )
    
    try:
        logger.info("Daemon configuration:")
        logger.info(f"  FTP Host: {config.host}:{config.port}")
        logger.info(f"  Remote Path: {config.remote_path}")
        logger.info(f"  Local Directory: {config.local_dir}")
        logger.info(f"  Poll Interval: {config.poll_interval}s")
        logger.info(f"  Max Concurrent Downloads: {config.max_concurrent_downloads}")
        logger.info("")
        logger.info("Press Ctrl+C to stop the daemon")
        logger.info("=" * 80)
        
        # Run daemon indefinitely (in production)
        # For testing, you can use: await daemon.run(max_iterations=5)
        await daemon.run()
        
    except KeyboardInterrupt:
        logger.info("\nShutdown requested by user")
        daemon.stop()
        logger.info("Daemon stopped gracefully")
        
    except Exception as e:
        logger.error(f"Fatal error in daemon: {e}", exc_info=True)
        raise
        
    finally:
        logger.info(f"Total files processed: {processor.processed_count}")
        logger.info("Daemon service terminated")


def main():
    """
    Main entry point for the daemon service.
    """
    # In production, you might want to:
    # 1. Parse command-line arguments for configuration
    # 2. Load configuration from a file
    # 3. Set up signal handlers for graceful shutdown
    # 4. Run as a systemd service
    
    try:
        asyncio.run(run_production_daemon())
    except KeyboardInterrupt:
        logger.info("Daemon interrupted")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        exit(1)


if __name__ == "__main__":
    # Configuration notes:
    logger.info("\n" + "=" * 80)
    logger.info("IMPORTANT: Update FTP credentials before running!")
    logger.info("=" * 80)
    logger.info("")
    logger.info("To run this example:")
    logger.info("1. Update FTP credentials in run_production_daemon()")
    logger.info("2. Ensure output directories exist and are writable")
    logger.info("3. Run: python ftp_integration_example.py")
    logger.info("")
    logger.info("For production deployment:")
    logger.info("1. Store credentials in environment variables or secrets manager")
    logger.info("2. Set up proper logging (file rotation, monitoring)")
    logger.info("3. Configure as a systemd service for automatic restart")
    logger.info("4. Set up monitoring and alerting")
    logger.info("=" * 80)
    logger.info("")
    
    # Uncomment to run:
    # main()
    
    logger.info("Example is ready to run. Uncomment main() to start.")
