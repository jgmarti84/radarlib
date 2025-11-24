# Visualization Examples

This folder contains examples for generating radar visualizations.

## Examples

- **colormap_usage_example.py** - Using custom colormaps with PyART
- **png_generation_example.py** - End-to-end PNG generation from BUFR files
- **geotiff_generation_example.py** - GeoTIFF generation with georeferencing

## Usage

```python
from radarlib.io.bufr import bufr_to_dict, bufr_to_pyart
from radarlib.io.pyart.radar_png_plotter import plot_and_save_ppi
```
