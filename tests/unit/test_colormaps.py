"""
Tests for custom colormap registration.

This module tests that custom colormaps are properly defined, generated,
and registered with matplotlib.
"""

import matplotlib.pyplot as plt
import pytest

from radarlib import colormaps


class TestColormapRegistration:
    """Test that colormaps are properly registered with matplotlib."""

    def test_init_cmaps_returns_dict(self):
        """Test that init_cmaps returns a dictionary of colormaps."""
        cmap_dict = colormaps.init_cmaps()
        assert isinstance(cmap_dict, dict)
        assert len(cmap_dict) > 0

    def test_vrad_colormap_in_datad(self):
        """Test that vrad colormap specification exists."""
        assert "vrad" in colormaps.datad
        assert isinstance(colormaps.datad["vrad"], dict)
        assert "red" in colormaps.datad["vrad"]
        assert "green" in colormaps.datad["vrad"]
        assert "blue" in colormaps.datad["vrad"]

    def test_vrad_colormap_registered(self):
        """Test that grc_vrad colormap is registered with matplotlib."""
        # Try to get the colormap from matplotlib
        cmap = plt.colormaps["grc_vrad"]
        assert cmap is not None
        assert cmap.name == "grc_vrad"

    def test_vrad_reversed_colormap_registered(self):
        """Test that grc_vrad_r (reversed) colormap is registered."""
        cmap_r = plt.colormaps["grc_vrad_r"]
        assert cmap_r is not None
        assert cmap_r.name == "grc_vrad_r"

    def test_registered_colormap_names_exported(self):
        """Test that REGISTERED_COLORMAP_NAMES is exported and contains expected names."""
        assert hasattr(colormaps, "REGISTERED_COLORMAP_NAMES")
        assert isinstance(colormaps.REGISTERED_COLORMAP_NAMES, list)
        assert "grc_vrad" in colormaps.REGISTERED_COLORMAP_NAMES
        assert "grc_vrad_r" in colormaps.REGISTERED_COLORMAP_NAMES

    def test_colormap_can_be_used_in_plot(self):
        """Test that registered colormap can be used in a matplotlib plot."""
        import numpy as np

        # Create some test data
        data = np.random.rand(10, 10)

        # Create a figure with the custom colormap
        fig, ax = plt.subplots()
        im = ax.imshow(data, cmap="grc_vrad")

        # Verify the colormap is correctly applied
        assert im.get_cmap().name == "grc_vrad"

        plt.close(fig)


class TestColormapSpecificationReverse:
    """Test colormap specification reversal."""

    def test_reverse_cmap_spec_returns_dict(self):
        """Test that _reverse_cmap_spec returns a dictionary."""
        original_spec = colormaps.datad["vrad"]
        reversed_spec = colormaps._reverse_cmap_spec(original_spec)
        assert isinstance(reversed_spec, dict)
        assert "red" in reversed_spec
        assert "green" in reversed_spec
        assert "blue" in reversed_spec

    def test_reverse_cmap_spec_inverts_positions(self):
        """Test that _reverse_cmap_spec properly inverts position values."""
        # Create a simple test spec
        test_spec = {
            "red": [(0.0, 0.0, 0.0), (1.0, 1.0, 1.0)],
            "green": [(0.0, 0.0, 0.0), (1.0, 1.0, 1.0)],
            "blue": [(0.0, 0.0, 0.0), (1.0, 1.0, 1.0)],
        }

        reversed_spec = colormaps._reverse_cmap_spec(test_spec)

        # First element should have position 0.0 (was 1.0)
        assert reversed_spec["red"][0][0] == 0.0
        # Last element should have position 1.0 (was 0.0)
        assert reversed_spec["red"][-1][0] == 1.0

    def test_reversed_spec_exists_in_datad(self):
        """Test that reversed specifications are added to datad."""
        # After init_cmaps is called, reversed specs should be in datad
        assert "vrad_r" in colormaps.datad


class TestColormapGeneration:
    """Test colormap generation from specifications."""

    def test_generate_cmap_returns_colormap(self):
        """Test that _generate_cmap returns a LinearSegmentedColormap."""
        from matplotlib.colors import LinearSegmentedColormap

        cmap = colormaps._generate_cmap("vrad", 256)
        assert isinstance(cmap, LinearSegmentedColormap)

    def test_generated_cmap_has_correct_name(self):
        """Test that generated colormap has the correct name."""
        cmap = colormaps._generate_cmap("vrad", 256)
        assert cmap.name == "vrad"

    def test_generated_cmap_callable(self):
        """Test that generated colormap can be called with values."""
        import numpy as np

        cmap = colormaps._generate_cmap("vrad", 256)

        # Test calling the colormap with a value
        color = cmap(0.5)
        assert isinstance(color, tuple)
        assert len(color) == 4  # RGBA

        # Test with array
        values = np.array([0.0, 0.25, 0.5, 0.75, 1.0])
        colors = cmap(values)
        assert colors.shape == (5, 4)  # 5 values, RGBA for each


class TestAutomaticRegistration:
    """Test that colormaps are automatically registered on import."""

    def test_colormaps_available_after_import(self):
        """Test that colormaps are available immediately after importing radarlib."""
        # When radarlib is imported, colormaps should be automatically registered
        # This is tested implicitly by the other tests, but we can verify explicitly
        try:
            cmap = plt.colormaps["grc_vrad"]
            assert cmap is not None
        except KeyError:
            pytest.fail("Colormap 'grc_vrad' should be registered automatically")

    def test_multiple_colormaps_registered(self):
        """Test that both normal and reversed versions are registered."""
        # Get all registered colormaps
        expected_names = ["grc_vrad", "grc_vrad_r"]

        for name in expected_names:
            try:
                cmap = plt.colormaps[name]
                assert cmap is not None, f"Colormap {name} should be registered"
            except KeyError:
                pytest.fail(f"Colormap '{name}' should be registered automatically")
