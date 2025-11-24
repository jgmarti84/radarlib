"""
Example: Using custom colormaps with PyART radar plotting.

This example demonstrates how custom colormaps are automatically registered
when radarlib is imported and can be used with PyART plotting functionality.

The custom colormaps are registered with a 'grc_' prefix and include both
normal and reversed (_r) versions.
"""

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

# Import radarlib - this automatically registers custom colormaps
import radarlib  # noqa: F401
from radarlib import colormaps
from radarlib.io.pyart.radar_png_plotter import FieldPlotConfig

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def example_list_registered_colormaps():
    """
    Example 1: List all registered custom colormaps.
    """
    logger.info("=" * 80)
    logger.info("Example 1: List Registered Custom Colormaps")
    logger.info("=" * 80)

    logger.info("\nRegistered custom colormaps:")
    for name in sorted(colormaps.REGISTERED_COLORMAP_NAMES):
        logger.info(f"  - {name}")

    logger.info("\nThese colormaps can be used anywhere matplotlib colormaps are accepted.")
    logger.info("")


def example_simple_plot_with_custom_colormap():
    """
    Example 2: Create a simple plot using custom colormap.
    """
    logger.info("=" * 80)
    logger.info("Example 2: Simple Plot with Custom Colormap")
    logger.info("=" * 80)

    # Create sample velocity data
    logger.info("\nCreating sample velocity data...")
    velocity_data = np.random.uniform(-15, 15, (100, 100))

    # Create figure with both normal and reversed colormaps
    logger.info("Creating plot with grc_vrad colormap...")
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    # Plot with normal colormap
    im1 = axes[0].imshow(velocity_data, cmap="grc_vrad", vmin=-15, vmax=15, aspect="auto")
    axes[0].set_title("Radial Velocity - grc_vrad (Normal)", fontsize=12, weight="bold")
    axes[0].set_xlabel("Range Gate")
    axes[0].set_ylabel("Azimuth")
    plt.colorbar(im1, ax=axes[0], label="Velocity (m/s)")

    # Plot with reversed colormap
    im2 = axes[1].imshow(velocity_data, cmap="grc_vrad_r", vmin=-15, vmax=15, aspect="auto")
    axes[1].set_title("Radial Velocity - grc_vrad_r (Reversed)", fontsize=12, weight="bold")
    axes[1].set_xlabel("Range Gate")
    axes[1].set_ylabel("Azimuth")
    plt.colorbar(im2, ax=axes[1], label="Velocity (m/s)")

    plt.tight_layout()

    # Save the plot
    output_dir = Path("outputs/colormap_examples")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "custom_colormap_example.png"

    plt.savefig(output_file, dpi=150, bbox_inches="tight")
    logger.info(f"✓ Plot saved to: {output_file}")
    plt.close(fig)
    logger.info("")


def example_using_colormap_with_field_plot_config():
    """
    Example 3: Use custom colormap with PyART FieldPlotConfig.
    """
    logger.info("=" * 80)
    logger.info("Example 3: Using Custom Colormap with FieldPlotConfig")
    logger.info("=" * 80)

    logger.info("\nCreating FieldPlotConfig with custom colormap...")

    # Create a configuration for velocity field using custom colormap
    velocity_config = FieldPlotConfig(field_name="VRAD", vmin=-15, vmax=15, cmap="grc_vrad")  # Use our custom colormap

    logger.info(f"  Field: {velocity_config.field_name}")
    logger.info(f"  Colormap: {velocity_config.cmap}")
    logger.info(f"  Value range: [{velocity_config.vmin}, {velocity_config.vmax}]")

    logger.info("\n✓ FieldPlotConfig created successfully!")
    logger.info("  This configuration can now be used with plot_and_save_ppi() function.")
    logger.info("")


def example_adding_new_colormaps():
    """
    Example 4: How to add new colormaps (documentation example).
    """
    logger.info("=" * 80)
    logger.info("Example 4: Adding New Custom Colormaps (Documentation)")
    logger.info("=" * 80)

    logger.info("\nTo add a new colormap, edit src/radarlib/colormaps.py:")
    logger.info("")
    logger.info("1. Define the colormap specification:")
    logger.info("   _my_colormap = {")
    logger.info("       'red':   [(0.0, r0, r0), (1.0, r1, r1)],")
    logger.info("       'green': [(0.0, g0, g0), (1.0, g1, g1)],")
    logger.info("       'blue':  [(0.0, b0, b0), (1.0, b1, b1)],")
    logger.info("   }")
    logger.info("")
    logger.info("2. Add it to the datad dictionary:")
    logger.info("   datad = {")
    logger.info("       'vrad': _vrad,")
    logger.info("       'my_colormap': _my_colormap,  # Add this line")
    logger.info("   }")
    logger.info("")
    logger.info("3. The colormap will be automatically registered as:")
    logger.info("   - grc_my_colormap (normal)")
    logger.info("   - grc_my_colormap_r (reversed)")
    logger.info("")
    logger.info("No other changes needed! The registration happens automatically.")
    logger.info("")


if __name__ == "__main__":
    logger.info("\n")
    logger.info("╔" + "=" * 78 + "╗")
    logger.info("║" + " " * 78 + "║")
    logger.info("║" + "  Custom Colormap Usage Examples".center(78) + "║")
    logger.info("║" + "  Automatic Registration with radarlib".center(78) + "║")
    logger.info("║" + " " * 78 + "║")
    logger.info("╚" + "=" * 78 + "╝")
    logger.info("")

    # Run examples
    try:
        example_list_registered_colormaps()
        example_simple_plot_with_custom_colormap()
        example_using_colormap_with_field_plot_config()
        example_adding_new_colormaps()

        logger.info("╔" + "=" * 78 + "╗")
        logger.info("║" + "  All examples completed successfully! ✓".center(78) + "║")
        logger.info("╚" + "=" * 78 + "╝")

    except Exception as e:
        logger.error(f"Error during examples: {e}", exc_info=True)
