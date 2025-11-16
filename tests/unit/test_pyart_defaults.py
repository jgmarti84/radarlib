"""Unit tests for radarlib.pyart_defaults module functions."""

import pytest

from radarlib import pyart_defaults


class TestVelocityLimit:
    """Test velocity_limit() function."""

    def test_default_limit_no_container(self):
        """Test default velocity limit when no container is provided."""
        result = pyart_defaults.velocity_limit(container=None)
        assert result == (-30.0, 30.0)

    def test_default_limit_with_selection(self):
        """Test default velocity limit with selection parameter."""
        result = pyart_defaults.velocity_limit(container=None, selection=0)
        assert result == (-30.0, 30.0)
        
        result = pyart_defaults.velocity_limit(container=None, selection=5)
        assert result == (-30.0, 30.0)

    def test_returns_tuple(self):
        """Test that function returns a tuple of two floats."""
        result = pyart_defaults.velocity_limit()
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], float)
        assert isinstance(result[1], float)

    def test_symmetric_limits(self):
        """Test that returned limits are symmetric (negative and positive of same value)."""
        result = pyart_defaults.velocity_limit()
        assert result[0] == -result[1]


class TestSpectrumWidthLimit:
    """Test spectrum_width_limit() function."""

    def test_default_limit_no_container(self):
        """Test default spectrum width limit when no container is provided."""
        result = pyart_defaults.spectrum_width_limit(container=None)
        assert result == (0, 30.0)

    def test_default_limit_with_selection(self):
        """Test default spectrum width limit with selection parameter."""
        result = pyart_defaults.spectrum_width_limit(container=None, selection=0)
        assert result == (0, 30.0)
        
        result = pyart_defaults.spectrum_width_limit(container=None, selection=5)
        assert result == (0, 30.0)

    def test_returns_tuple(self):
        """Test that function returns a tuple of two numbers."""
        result = pyart_defaults.spectrum_width_limit()
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_lower_limit_zero(self):
        """Test that lower limit is always zero (spectrum width can't be negative)."""
        result = pyart_defaults.spectrum_width_limit()
        assert result[0] == 0

    def test_upper_limit_positive(self):
        """Test that upper limit is positive."""
        result = pyart_defaults.spectrum_width_limit()
        assert result[1] > 0


class TestModuleConstants:
    """Test module-level constants and data structures."""

    def test_fill_value_constant(self):
        """Test that fill_value constant is defined."""
        assert hasattr(pyart_defaults, 'fill_value')
        assert pyart_defaults.fill_value == -9999.0

    def test_fill_value_uppercase(self):
        """Test that FILL_VALUE constant is defined."""
        assert hasattr(pyart_defaults, 'FILL_VALUE')
        assert pyart_defaults.FILL_VALUE == pyart_defaults.fill_value

    def test_line_large_constant(self):
        """Test that LINE_LARGE constant is defined."""
        assert hasattr(pyart_defaults, 'LINE_LARGE')
        assert pyart_defaults.LINE_LARGE == 90

    def test_line_format_constant(self):
        """Test that LINE_FORMAT constant is defined."""
        assert hasattr(pyart_defaults, 'LINE_FORMAT')
        assert pyart_defaults.LINE_FORMAT == "90.90"

    def test_field_name_constants(self):
        """Test that basic field name constants are defined."""
        assert hasattr(pyart_defaults, 'reflectivity')
        assert hasattr(pyart_defaults, 'velocity')
        assert hasattr(pyart_defaults, 'spectrum_width')
        assert hasattr(pyart_defaults, 'differential_reflectivity')
        assert hasattr(pyart_defaults, 'cross_correlation_ratio')


class TestDefaultFieldNames:
    """Test DEFAULT_FIELD_NAMES dictionary."""

    def test_default_field_names_exists(self):
        """Test that DEFAULT_FIELD_NAMES dictionary exists."""
        assert hasattr(pyart_defaults, 'DEFAULT_FIELD_NAMES')
        assert isinstance(pyart_defaults.DEFAULT_FIELD_NAMES, dict)

    def test_default_field_names_not_empty(self):
        """Test that DEFAULT_FIELD_NAMES is not empty."""
        assert len(pyart_defaults.DEFAULT_FIELD_NAMES) > 0

    def test_reflectivity_field_mapping(self):
        """Test that reflectivity field is mapped."""
        assert 'reflectivity' in pyart_defaults.DEFAULT_FIELD_NAMES
        assert pyart_defaults.DEFAULT_FIELD_NAMES['reflectivity'] == pyart_defaults.reflectivity

    def test_velocity_field_mapping(self):
        """Test that velocity field is mapped."""
        assert 'velocity' in pyart_defaults.DEFAULT_FIELD_NAMES
        assert pyart_defaults.DEFAULT_FIELD_NAMES['velocity'] == pyart_defaults.velocity


class TestDefaultFieldColormap:
    """Test DEFAULT_FIELD_COLORMAP dictionary."""

    def test_default_field_colormap_exists(self):
        """Test that DEFAULT_FIELD_COLORMAP dictionary exists."""
        assert hasattr(pyart_defaults, 'DEFAULT_FIELD_COLORMAP')
        assert isinstance(pyart_defaults.DEFAULT_FIELD_COLORMAP, dict)

    def test_default_field_colormap_not_empty(self):
        """Test that DEFAULT_FIELD_COLORMAP is not empty."""
        assert len(pyart_defaults.DEFAULT_FIELD_COLORMAP) > 0

    def test_colormap_values_are_strings(self):
        """Test that colormap values are strings."""
        for value in list(pyart_defaults.DEFAULT_FIELD_COLORMAP.values())[:5]:
            assert isinstance(value, str)


class TestDefaultFieldLimits:
    """Test DEFAULT_FIELD_LIMITS dictionary."""

    def test_default_field_limits_exists(self):
        """Test that DEFAULT_FIELD_LIMITS dictionary exists."""
        assert hasattr(pyart_defaults, 'DEFAULT_FIELD_LIMITS')
        assert isinstance(pyart_defaults.DEFAULT_FIELD_LIMITS, dict)

    def test_default_field_limits_not_empty(self):
        """Test that DEFAULT_FIELD_LIMITS is not empty."""
        assert len(pyart_defaults.DEFAULT_FIELD_LIMITS) > 0

    def test_limit_values_are_tuples_or_callables(self):
        """Test that limit values are either tuples or callables."""
        for value in pyart_defaults.DEFAULT_FIELD_LIMITS.values():
            assert isinstance(value, (tuple, type(lambda: None)))

    def test_reflectivity_limits(self):
        """Test specific reflectivity limits."""
        # reflectivity field name is "DBZH" in this configuration
        assert pyart_defaults.reflectivity in pyart_defaults.DEFAULT_FIELD_LIMITS
        limits = pyart_defaults.DEFAULT_FIELD_LIMITS[pyart_defaults.reflectivity]
        assert limits == (-10.0, 65.0)


class TestDefaultMetadata:
    """Test DEFAULT_METADATA dictionary."""

    def test_default_metadata_exists(self):
        """Test that DEFAULT_METADATA dictionary exists."""
        assert hasattr(pyart_defaults, 'DEFAULT_METADATA')
        assert isinstance(pyart_defaults.DEFAULT_METADATA, dict)

    def test_default_metadata_not_empty(self):
        """Test that DEFAULT_METADATA is not empty."""
        assert len(pyart_defaults.DEFAULT_METADATA) > 0

    def test_metadata_values_are_dicts(self):
        """Test that metadata values are dictionaries."""
        for value in list(pyart_defaults.DEFAULT_METADATA.values())[:5]:
            assert isinstance(value, dict)


class TestFieldMappings:
    """Test FIELD_MAPPINGS dictionary."""

    def test_field_mappings_exists(self):
        """Test that FIELD_MAPPINGS dictionary exists."""
        assert hasattr(pyart_defaults, 'FIELD_MAPPINGS')
        assert isinstance(pyart_defaults.FIELD_MAPPINGS, dict)

    def test_field_mappings_not_empty(self):
        """Test that FIELD_MAPPINGS is not empty."""
        assert len(pyart_defaults.FIELD_MAPPINGS) > 0

    def test_expected_mapping_keys(self):
        """Test that expected mapping keys exist."""
        expected_keys = ['sigmet', 'nexrad_archive', 'cfradial', 'uf']
        for key in expected_keys:
            assert key in pyart_defaults.FIELD_MAPPINGS
