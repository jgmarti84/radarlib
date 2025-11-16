# Custom Colormaps

This document describes the custom colormap system in radarlib.

## Overview

Radarlib automatically registers custom colormaps with matplotlib when the package is imported. These colormaps are specifically designed for radar data visualization and are available for use with any matplotlib or PyART plotting functionality.

## Automatic Registration

When you import radarlib, custom colormaps are automatically registered with matplotlib:

```python
import radarlib

# Colormaps are now registered and ready to use
import matplotlib.pyplot as plt
plt.imshow(data, cmap='grc_vrad')
```

## Available Colormaps

All custom colormaps are registered with the `grc_` prefix (Grupo Radar Córdoba). Each colormap has both a normal and reversed version:

- **grc_vrad**: Custom velocity colormap for radial velocity fields
- **grc_vrad_r**: Reversed version of the velocity colormap

## Using Custom Colormaps

### With Matplotlib

```python
import matplotlib.pyplot as plt
import numpy as np
import radarlib  # Registers colormaps automatically

data = np.random.rand(10, 10)
plt.imshow(data, cmap='grc_vrad')
plt.colorbar()
plt.show()
```

### With PyART Plotting

```python
from radarlib.io.pyart.radar_png_plotter import FieldPlotConfig, plot_and_save_ppi

# Create field configuration with custom colormap
field_config = FieldPlotConfig(
    field_name='VRAD',
    vmin=-15,
    vmax=15,
    cmap='grc_vrad'  # Use custom colormap
)

# Use in plotting
plot_and_save_ppi(
    radar=radar,
    field='VRAD',
    output_path='output/',
    filename='velocity.png',
    field_config=field_config
)
```

## Adding New Colormaps

To add a new custom colormap:

1. Edit `src/radarlib/colormaps.py`

2. Define your colormap specification:

```python
_my_colormap = {
    'red':   [(0.0, 0.0, 0.0), (0.5, 1.0, 1.0), (1.0, 1.0, 1.0)],
    'green': [(0.0, 0.0, 0.0), (0.5, 0.0, 0.0), (1.0, 1.0, 1.0)],
    'blue':  [(0.0, 1.0, 1.0), (0.5, 0.0, 0.0), (1.0, 0.0, 0.0)],
}
```

3. Add it to the `datad` dictionary:

```python
datad = {
    'vrad': _vrad,
    'my_colormap': _my_colormap,  # Add your colormap here
}
```

4. Your colormap will be automatically registered as:
   - `grc_my_colormap` (normal version)
   - `grc_my_colormap_r` (reversed version)

No other changes are needed! The registration happens automatically when radarlib is imported.

## Colormap Specification Format

Colormaps are defined using matplotlib's `LinearSegmentedColormap` format. Each color channel (red, green, blue) is defined by a list of tuples:

```python
'red': [(position, value_before, value_after), ...]
```

Where:
- `position`: float between 0.0 and 1.0, indicating position in the colormap
- `value_before`: color value (0.0 to 1.0) just before this position
- `value_after`: color value (0.0 to 1.0) just after this position

For continuous colormaps, `value_before` and `value_after` are typically the same.

## Implementation Details

The colormap registration system:

1. Reads colormap specifications from the `datad` dictionary
2. Automatically generates reversed versions using `_reverse_cmap_spec()`
3. Creates `LinearSegmentedColormap` objects with the configured LUT size
4. Registers all colormaps with matplotlib using `plt.colormaps.register()`
5. Exports the list of registered names in `REGISTERED_COLORMAP_NAMES`

All of this happens automatically when radarlib is imported, requiring no manual intervention from the user.

## Examples

See `examples/colormap_usage_example.py` for complete usage examples including:
- Listing registered colormaps
- Creating plots with custom colormaps
- Using colormaps with PyART configurations
- Adding new colormaps

Run the example:

```bash
python examples/colormap_usage_example.py
```
