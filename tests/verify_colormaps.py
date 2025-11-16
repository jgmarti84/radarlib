#!/usr/bin/env python
"""
Verification script to demonstrate the automatic colormap registration system.

This script verifies that:
1. Colormaps are automatically registered when radarlib is imported
2. They can be accessed via matplotlib
3. Both normal and reversed versions are available
4. They work with PyART plotting configurations
"""

import sys


def verify_automatic_registration():
    """Verify that colormaps are automatically registered."""
    print("=" * 80)
    print("VERIFICATION: Automatic Colormap Registration")
    print("=" * 80)
    print()

    # Import radarlib - this should automatically register colormaps
    print("1. Importing radarlib...")
    import radarlib  # noqa: F401

    print("   ✓ radarlib imported successfully")
    print()

    # Check that colormaps are registered
    print("2. Checking colormap registration...")
    import matplotlib.pyplot as plt

    expected_colormaps = ["grc_vrad", "grc_vrad_r"]
    all_registered = True

    for name in expected_colormaps:
        try:
            cmap = plt.colormaps[name]
            print(f"   ✓ {name} is registered (type: {type(cmap).__name__})")
        except KeyError:
            print(f"   ✗ {name} is NOT registered")
            all_registered = False

    if not all_registered:
        print("\n❌ FAILED: Not all colormaps were registered")
        return False

    print()
    print("3. Testing colormap functionality...")

    # Test that colormap can be used
    import numpy as np

    data = np.random.rand(10, 10)
    fig, ax = plt.subplots()
    im = ax.imshow(data, cmap="grc_vrad")

    if im.get_cmap().name == "grc_vrad":
        print("   ✓ Colormap can be applied to plots")
    else:
        print("   ✗ Colormap not correctly applied")
        plt.close(fig)
        return False

    plt.close(fig)
    print()

    # Test with PyART configuration
    print("4. Testing with PyART FieldPlotConfig...")
    from radarlib.io.pyart.radar_png_plotter import FieldPlotConfig

    try:
        config = FieldPlotConfig(field_name="VRAD", vmin=-15, vmax=15, cmap="grc_vrad")
        if config.cmap == "grc_vrad":
            print("   ✓ Colormap works with FieldPlotConfig")
        else:
            print("   ✗ Colormap not correctly stored in FieldPlotConfig")
            return False
    except Exception as e:
        print(f"   ✗ Error creating FieldPlotConfig: {e}")
        return False

    print()
    print("=" * 80)
    print("✓ ALL VERIFICATIONS PASSED")
    print("=" * 80)
    print()
    print("The automatic colormap registration system is working correctly!")
    print()

    return True


if __name__ == "__main__":
    success = verify_automatic_registration()
    sys.exit(0 if success else 1)
