# -*- coding: utf-8 -*-
"""
Tests for the refactored COLMAX generation.
"""
from pathlib import Path

import pytest

from radarlib.io.pyart.colmax import generate_colmax
from radarlib.io.pyart.pyart_radar import estandarizar_campos_RMA, read_radar_netcdf


@pytest.fixture
def radar_object(sample_RMA11_vol1_bufr_files):
    """Load and standardize example radar for testing."""
    netcdf_fname = Path(__file__).parent.parent.parent / "outputs/example_netcdfs/RMA11_0315_01_20251020T152828Z.nc"
    if not netcdf_fname.exists():
        from radarlib.io.bufr.pyart_writer import bufr_paths_to_pyart

        # build the radar object from the bufr files
        save_path = Path(__file__).parent.parent.parent / "outputs/example_netcdfs/"
        radar = bufr_paths_to_pyart(
            [str(fn) for fn in sample_RMA11_vol1_bufr_files], root_resources=None, save_path=save_path
        )

    radar = read_radar_netcdf(str(netcdf_fname))
    radar = estandarizar_campos_RMA(radar=radar, debug=False, idioma=0)  # type: ignore
    return radar


class TestGenerateColmax:
    """Test suite for COLMAX generation."""

    def test_colmax_basic(self, radar_object):
        """Test basic COLMAX generation."""
        radar = radar_object
        assert radar.nsweeps > 1, "Test radar must have multiple sweeps"

        result_radar = generate_colmax(radar=radar)

        assert result_radar is not None, "COLMAX generation should succeed"
        assert "COLMAX" in result_radar.fields, "COLMAX field should be added to radar"
        # Verify that the original radar is not modified
        assert "COLMAX" not in radar.fields, "Original radar should not be modified"

    def test_colmax_with_filters(self, radar_object):
        """Test COLMAX generation with polarimetric filters."""
        radar = radar_object

        result_radar = generate_colmax(
            radar=radar,
            refl_filter=True,
            refl_threshold=10,
            rhv_filter=True,
            rhv_threshold=0.9,
            wrad_filter=True,
            wrad_threshold=4.2,
            zdr_filter=True,
            zdr_threshold=8.5,
        )

        assert result_radar is not None
        assert "COLMAX" in result_radar.fields
        # Verify that the original radar is not modified
        assert "COLMAX" not in radar.fields

    def test_colmax_custom_source_field(self, radar_object):
        """Test COLMAX with custom source field."""
        radar = radar_object

        # Use DBZH as source if available
        if "DBZH" in radar.fields:
            result_radar = generate_colmax(
                radar=radar,
                source_field="DBZH",
                target_field="colmax_dbzh",
            )
            assert result_radar is not None
            assert "colmax_dbzh" in result_radar.fields

    # def test_colmax_insufficient_sweeps(self):
    #     """Test that COLMAX fails gracefully with single sweep."""
    #     pytest.importorskip("pyart")
    #     from pyart.core import Radar as PyartRadar
    #     import numpy as np

    #     # Create minimal single-sweep radar
    #     radar = PyartRadar(
    #         time={"data": np.array([0]), "units": "seconds since 2020-01-01"},
    #         metadata={},
    #         latitude={"data": np.array([0])},
    #         longitude={"data": np.array([0])},
    #         altitude={"data": np.array([0])},
    #     )
    #     radar.nsweeps = 1
    #     radar.nrays = 360
    #     radar.ngates = 100

    #     result = generate_colmax(radar=radar)
    #     assert result is False, "COLMAX should fail with single sweep"

    def test_colmax_missing_source_field(self, radar_object):
        """Test that COLMAX fails gracefully with missing source field."""
        radar = radar_object

        result_radar = generate_colmax(
            radar=radar,
            source_field="NONEXISTENT_FIELD",
        )

        assert result_radar is None, "COLMAX should fail with missing field"

    def test_colmax_field_shape(self, radar_object):
        """Test that COLMAX field has correct shape."""
        radar = radar_object

        result_radar = generate_colmax(radar=radar)

        colmax_field = result_radar.fields["COLMAX"]["data"]
        assert colmax_field.shape == (result_radar.nrays, result_radar.ngates)

    def test_colmax_metadata(self, radar_object):
        """Test that COLMAX field has proper metadata."""
        radar = radar_object

        result_radar = generate_colmax(radar=radar)

        colmax_meta = result_radar.fields["COLMAX"]
        assert "standard_name" in colmax_meta
        assert "long_name" in colmax_meta
        assert "units" in colmax_meta
        assert colmax_meta["standard_name"] == "COLMAX"
        assert colmax_meta["long_name"] == "Column Maximum"

    def test_colmax_no_filters(self, radar_object):
        """Test COLMAX generation with all filters disabled."""
        radar = radar_object

        # Run with no filters - should use source field directly
        result_radar = generate_colmax(
            radar=radar,
            refl_filter=False,
            rhv_filter=False,
            wrad_filter=False,
            zdr_filter=False,
        )

        assert result_radar is not None
        assert "COLMAX" in result_radar.fields
        # Verify that the original radar is not modified
        assert "COLMAX" not in radar.fields

    def test_colmax_returns_new_object(self, radar_object):
        """Test that generate_colmax returns a new radar object."""
        radar = radar_object
        original_field_count = len(radar.fields)

        result_radar = generate_colmax(radar=radar)

        # Original radar should not be modified
        assert len(radar.fields) == original_field_count
        assert "COLMAX" not in radar.fields

        # Result radar should have the new field
        assert result_radar is not None
        assert "COLMAX" in result_radar.fields
        assert len(result_radar.fields) == original_field_count + 1

        # They should be different objects
        assert radar is not result_radar
