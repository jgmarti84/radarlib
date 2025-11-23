import asyncio
import datetime
import os
from pathlib import Path

from radarlib import config
from radarlib.io.ftp.continuous_daemon import ContinuousDaemon, ContinuousDaemonConfig

if __name__ == "__main__":
    # Example configuration
    radar_name = "RMA1"
    vol_types = {}
    vol_types["0315"] = {"01": ["DBZH", "DBZV", "ZDR", "RHOHV", "PHIDP", "KDP"], "02": ["VRAD", "WRAD"]}
    vol_types["9202"] = {"01": ["DBZH", "DBZV", "ZDR", "RHOHV", "PHIDP", "KDP"], "02": ["VRAD", "WRAD"]}

    config = ContinuousDaemonConfig(
        host=config.FTP_HOST,
        username=config.FTP_USER,
        password=config.FTP_PASS,
        radar_name=radar_name,
        remote_base_path=f"/L2/{radar_name}",
        start_date=datetime.datetime(2025, 11, 23, 13, 0, 0, tzinfo=datetime.timezone.utc),
        local_bufr_dir=Path(os.path.join(config.ROOT_RADAR_FILES_PATH, radar_name, "bufr")),
        state_db=Path(os.path.join(config.ROOT_RADAR_FILES_PATH, radar_name, "state.db")),
        poll_interval=30,  # Check every 30 seconds
        vol_types=vol_types,
    )

    daemon = ContinuousDaemon(config)

    try:
        asyncio.run(daemon.run_service())
    except KeyboardInterrupt:
        print("Daemon stopped by user.")
