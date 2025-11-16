# -*- coding: utf-8 -*-
"""
Tests for the refactored COLMAX generation.
"""
import pytest
from pathlib import Path

from radarlib.io.pyart.pyart_radar import (
    read_radar_netcdf,
    estandarizar_campos_RMA,
)
from radarlib.io.pyart.colmax import generate_colmax


@pytest.fixture
def radar_object():
    """Load and standardize example radar for testing."""
    netcdf_fname = Path(__file__).parent.parent.parent / (
        "outputs/example_netcdfs/RMA11_0315_01_20251020T151109Z.nc"
    )
    
    if not netcdf_fname.exists():
        pytest.skip(f"Example netCDF file not found: {netcdf_fname}")
    
    radar = read_radar_netcdf(str(netcdf_fname))
    radar = estandarizar_campos_RMA(radar=radar, debug=False, idioma=0)#type: ignore
    return radar


class TestGenerateColmax:
    """Test suite for COLMAX generation."""
    
    def test_colmax_basic(self, radar_object):
        """Test basic COLMAX generation."""
        radar = radar_object
        assert radar.nsweeps > 1, "Test radar must have multiple sweeps"
        
        result = generate_colmax(radar=radar)
        
        assert result is True, "COLMAX generation should succeed"
        assert "COLMAX" in radar.fields, "COLMAX field should be added to radar"
    
    def test_colmax_with_filters(self, radar_object):
        """Test COLMAX generation with polarimetric filters."""
        radar = radar_object
        
        result = generate_colmax(
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
        
        assert result is True
        assert "COLMAX" in radar.fields
    
    def test_colmax_custom_source_field(self, radar_object):
        """Test COLMAX with custom source field."""
        radar = radar_object
        
        # Use DBZH as source if available
        if "DBZH" in radar.fields:
            result = generate_colmax(
                radar=radar,
                source_field="DBZH",
                target_field="colmax_dbzh",
            )
            assert result is True
            assert "colmax_dbzh" in radar.fields
    
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
        
        result = generate_colmax(
            radar=radar,
            source_field="NONEXISTENT_FIELD",
        )
        
        assert result is False, "COLMAX should fail with missing field"
    
    def test_colmax_field_shape(self, radar_object):
        """Test that COLMAX field has correct shape."""
        radar = radar_object
        
        generate_colmax(radar=radar)
        
        colmax_field = radar.fields["COLMAX"]["data"]
        assert colmax_field.shape == (radar.nrays, radar.ngates)
    
    def test_colmax_metadata(self, radar_object):
        """Test that COLMAX field has proper metadata."""
        radar = radar_object
        
        generate_colmax(radar=radar)
        
        colmax_meta = radar.fields["COLMAX"]
        assert "standard_name" in colmax_meta
        assert "long_name" in colmax_meta
        assert "units" in colmax_meta
        assert colmax_meta["standard_name"] == "COLMAX"
        assert colmax_meta["long_name"] == "Column Maximum"