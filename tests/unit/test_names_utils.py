"""Unit tests for radarlib.utils.names_utils module."""

import datetime
import os
from datetime import timezone
from unittest.mock import patch

import pytest

from radarlib.utils import names_utils


class TestGetTimeFromRMAFilename:
    """Test get_time_from_RMA_filename() function.
    
    Note: The function expects format RADAR_ELEV_SWEEP_TIMESTAMP.ext (4 parts),
    but actual BUFR files use RADAR_ELEV_SWEEP_FIELD_TIMESTAMP.ext (5 parts).
    """

    def test_basic_filename_parsing_4_parts(self):
        """Test parsing RMA filename with 4-part format (without field name)."""
        # This is the format the function currently expects
        filename = "RMA5_0315_1_20240101T120000Z.bufr"
        result = names_utils.get_time_from_RMA_filename(filename)

        assert isinstance(result, datetime.datetime)
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 1
        assert result.hour == 12
        assert result.minute == 0
        assert result.second == 0
        assert result.tzinfo == timezone.utc

    def test_different_datetime_values_4_parts(self):
        """Test parsing with different datetime values (4-part format)."""
        filename = "AR5_1000_2_20231225T235959Z.bufr"
        result = names_utils.get_time_from_RMA_filename(filename)

        assert result.year == 2023
        assert result.month == 12
        assert result.day == 25
        assert result.hour == 23
        assert result.minute == 59
        assert result.second == 59

    def test_timezone_utc_true(self):
        """Test that tz_UTC=True returns UTC timezone."""
        filename = "RMA1_0315_1_20240615T143022Z.bufr"
        result = names_utils.get_time_from_RMA_filename(filename, tz_UTC=True)

        assert result.tzinfo == timezone.utc

    def test_timezone_utc_false(self):
        """Test that tz_UTC=False returns Argentina timezone."""
        filename = "RMA1_0315_1_20240615T143022Z.bufr"
        result = names_utils.get_time_from_RMA_filename(filename, tz_UTC=False)

        # Should be converted to Argentina timezone
        assert result.tzinfo is not None
        assert result.tzinfo != timezone.utc

    def test_filename_without_extension(self):
        """Test parsing filename without .bufr extension."""
        filename = "RMA5_0315_1_20240101T120000Z"
        result = names_utils.get_time_from_RMA_filename(filename)

        assert result.year == 2024
        assert result.month == 1
        assert result.day == 1


class TestGetPathFromRMAFilename:
    """Test get_path_from_RMA_filename() function.
    
    Note: This function expects 4-part format: RADAR_ELEV_SWEEP_TIMESTAMP.ext
    (without field name). The actual BUFR files have 5 parts but this function
    is not actively used in the codebase (all usage is commented out).
    """

    def test_basic_path_generation_4_parts(self):
        """Test generating path from RMA filename with 4-part format."""
        filename = "RMA5_0315_1_20240615T143022Z.bufr"
        result = names_utils.get_path_from_RMA_filename(filename)

        # Should contain radar name
        assert "RMA5" in result
        # Should contain year
        assert "2024" in result
        # Should contain month
        assert "06" in result
        # Should contain day
        assert "15" in result
        # Should contain hour
        assert "14" in result

    def test_path_structure_4_parts(self):
        """Test that path has correct structure."""
        filename = "AR5_1000_2_20231225T235959Z.bufr"
        result = names_utils.get_path_from_RMA_filename(filename)

        # Path should follow pattern: .../radar/year/month/day/hour
        parts = result.split(os.sep)
        assert "AR5" in parts
        assert "2023" in parts
        assert "12" in parts
        assert "25" in parts
        assert "23" in parts

    def test_custom_root_radar_files_4_parts(self):
        """Test using custom root_radar_files parameter."""
        filename = "RMA1_0315_1_20240101T120000Z.bufr"
        custom_root = "/custom/radar/root"
        result = names_utils.get_path_from_RMA_filename(filename, root_radar_files=custom_root)

        assert result.startswith(custom_root)

    def test_different_radars_4_parts(self):
        """Test path generation for different radar names."""
        filenames = [
            "RMA1_0315_1_20240101T120000Z.bufr",
            "RMA5_0315_1_20240101T120000Z.bufr",
            "AR5_1000_1_20240101T120000Z.bufr",
        ]

        for filename in filenames:
            result = names_utils.get_path_from_RMA_filename(filename)
            radar_name = filename.split("_")[0]
            assert radar_name in result


class TestGetNetcdfFilenameFromBufrFilename:
    """Test get_netcdf_filename_from_bufr_filename() function.
    
    The function creates format: RADAR_ELEV_SWEEP_TIMESTAMP.nc from
    input format: RADAR_ELEV_SWEEP_FIELD_TIMESTAMP.ext
    (i.e., it skips the field name in the middle)
    """

    def test_basic_conversion(self):
        """Test basic BUFR to NetCDF filename conversion."""
        bufr_filename = "RMA5_0315_1_DBZH_20240101T120000Z.bufr"
        result = names_utils.get_netcdf_filename_from_bufr_filename(bufr_filename)

        # Function skips field name and creates: RMA5_0315_1_20240101T120000Z.nc
        assert result == "RMA5_0315_1_20240101T120000Z.nc"
        assert result.endswith(".nc")
        assert "RMA5" in result
        assert "0315" in result
        assert "1" in result
        assert "20240101T120000Z" in result

    def test_removes_extension(self):
        """Test that .bufr extension is removed."""
        bufr_filename = "AR5_1000_2_VRAD_20231225T235959Z.bufr"
        result = names_utils.get_netcdf_filename_from_bufr_filename(bufr_filename)

        assert not result.endswith(".bufr")
        assert result.endswith(".nc")
        assert result == "AR5_1000_2_20231225T235959Z.nc"

    def test_field_name_not_in_output(self):
        """Test that field name is not included in output (intentional behavior)."""
        bufr_filename = "RMA1_0315_3_ZDR_20240615T143022Z.bufr"
        result = names_utils.get_netcdf_filename_from_bufr_filename(bufr_filename)

        # Field name "ZDR" should NOT be in the output
        # (the function skips parts[3] which is the field name)
        assert "ZDR" not in result
        assert result == "RMA1_0315_3_20240615T143022Z.nc"

    def test_timestamp_preserved(self):
        """Test that timestamp is preserved in output."""
        bufr_filename = "RMA5_0315_1_DBZH_20240101T120000Z.bufr"
        result = names_utils.get_netcdf_filename_from_bufr_filename(bufr_filename)
        
        assert "20240101T120000Z" in result
        assert result.endswith(".nc")


class TestTimezones:
    """Test timezone handling."""

    def test_utc_timezone_constant(self):
        """Test that UTC timezone constant is defined."""
        assert hasattr(names_utils, "tz_utc")
        assert names_utils.tz_utc is not None

    def test_argentina_timezone_constant(self):
        """Test that Argentina timezone constant is defined."""
        assert hasattr(names_utils, "tz_arg")
        assert names_utils.tz_arg is not None
