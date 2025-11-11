import numpy as np
import zlib
import pytest

from radarlib.io.bufr import bufr as bufr_mod


def test_decompress_sweep_roundtrip(sample_sweep_bytes):
    sweep = dict(sample_sweep_bytes)
    # The module's decompress_sweep expects a dict with 'compress_data', 'nrays', 'ngates'
    out = bufr_mod.decompress_sweep(sweep)
    assert out.shape == (sweep["nrays"], sweep["ngates"])
    assert pytest.approx(out[0, 0]) == 0.5


def test_uniformize_and_assemble():
    a = np.ones((2, 3))
    b = np.full((2, 5), 2.0)
    sweeps = [{"data": a.copy(), "ngates": 3}, {"data": b.copy(), "ngates": 5}]
    sweeps = bufr_mod.uniformize_sweeps(sweeps)
    assert sweeps[0]["ngates"] == 5
    vol = bufr_mod.assemble_volume(sweeps)
    assert vol.shape == (4, 5)
