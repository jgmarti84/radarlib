#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Example: Basic FTP Client Usage

This example demonstrates how to use the FTPClient class to interact with
an FTP server and download BUFR files.
"""

from pathlib import Path

from radarlib import config
from radarlib.io.ftp import FTPClient

# Configuration
FTP_HOST = config.FTP_HOST
FTP_USER = config.FTP_USER
FTP_PASSWORD = config.FTP_PASS
REMOTE_DIR = "/L2/RMA1/2024/01/01/00/0019"
LOCAL_DIR = Path("./downloads")


def main():
    """Main example function."""
    print("=" * 60)
    print("FTP Client Example")
    print("=" * 60)

    # Create local download directory
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)

    # Initialize FTP client
    client = FTPClient(host=FTP_HOST, user=FTP_USER, password=FTP_PASSWORD)
    print(f"\nConnected to FTP server: {FTP_HOST}")

    # Example 1: List files in a directory
    print(f"\nExample 1: Listing files in {REMOTE_DIR}")
    print("-" * 60)
    try:
        files = client.list_files(REMOTE_DIR)
        print(f"Found {len(files)} files:")
        for i, file in enumerate(files[:10], 1):  # Show first 10
            print(f"  {i}. {file}")
        if len(files) > 10:
            print(f"  ... and {len(files) - 10} more")
    except Exception as e:
        print(f"Error listing files: {e}")

    # Example 2: Download a single file
    print("\nExample 2: Download a single BUFR file")
    print("-" * 60)
    try:
        # Pick first BUFR file if available
        bufr_files = [f for f in files if f.endswith(".BUFR")]
        if bufr_files:
            filename = bufr_files[0]
            remote_path = f"{REMOTE_DIR}/{filename}"
            local_path = LOCAL_DIR / filename

            print(f"Downloading: {filename}")
            client.download_file(remote_path, local_path)
            print(f"Saved to: {local_path}")
        else:
            print("No BUFR files found in directory")
    except Exception as e:
        print(f"Error downloading file: {e}")

    # Example 3: Download multiple files
    print("\nExample 3: Download multiple files at once")
    print("-" * 60)
    try:
        # Download first 3 BUFR files
        files_to_download = bufr_files[:3]
        if files_to_download:
            print(f"Downloading {len(files_to_download)} files...")
            client.download_files(REMOTE_DIR, files_to_download, LOCAL_DIR)
            print("All files downloaded successfully")
        else:
            print("No files to download")
    except Exception as e:
        print(f"Error downloading files: {e}")

    # Example 4: Check if a file exists
    print("\nExample 4: Check if a file exists on server")
    print("-" * 60)
    if bufr_files:
        test_file = f"{REMOTE_DIR}/{bufr_files[0]}"
        exists = client.file_exists(test_file)
        print(f"File '{bufr_files[0]}' exists: {exists}")

        fake_file = f"{REMOTE_DIR}/nonexistent_file.BUFR"
        exists = client.file_exists(fake_file)
        print(f"File 'nonexistent_file.BUFR' exists: {exists}")

    print("\n" + "=" * 60)
    print("Example complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
