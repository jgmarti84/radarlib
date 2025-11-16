"""
Integration test to verify custom colormaps work with PyART plotting.

This test creates a simple test to verify that the registered colormaps
can be used in PyART plotting functionality.
"""

import matplotlib.pyplot as plt
import numpy as np
import pytest


def test_colormap_available_for_plotting():
    """Test that custom colormap can be accessed and used."""
    # Import radarlib to trigger colormap registration
    import radarlib  # noqa: F401

    # Verify the colormap is registered
    assert "grc_vrad" in plt.colormaps
    assert "grc_vrad_r" in plt.colormaps

    # Create a simple plot using the colormap
    data = np.random.rand(10, 10)
    fig, ax = plt.subplots()
    im = ax.imshow(data, cmap="grc_vrad")

    # Verify the colormap is applied
    assert im.get_cmap().name == "grc_vrad"

    plt.close(fig)


def test_colormap_with_pyart_field_plot_config():
    """Test that custom colormap works with FieldPlotConfig."""
    import radarlib  # noqa: F401
    from radarlib.io.pyart.radar_png_plotter import FieldPlotConfig

    # Create a FieldPlotConfig with custom colormap
    config = FieldPlotConfig(field_name="VRAD", vmin=-15, vmax=15, cmap="grc_vrad")  # Use our custom colormap

    # Verify the configuration was created successfully
    assert config.field_name == "VRAD"
    assert config.cmap == "grc_vrad"
    assert config.vmin == -15
    assert config.vmax == 15


def test_reversed_colormap_available():
    """Test that reversed version of colormap is also available."""
    import radarlib  # noqa: F401

    # Verify both normal and reversed are registered
    assert "grc_vrad" in plt.colormaps
    assert "grc_vrad_r" in plt.colormaps

    # Get the colormaps
    cmap = plt.colormaps["grc_vrad"]
    cmap_r = plt.colormaps["grc_vrad_r"]

    # They should be different objects
    assert cmap.name != cmap_r.name

    # Test that they produce different colors at the same position
    color_normal = cmap(0.0)
    color_reversed = cmap_r(0.0)

    # At position 0, normal and reversed should give different results
    # (unless the colormap is symmetric, which vrad is not)
    assert color_normal != color_reversed


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
