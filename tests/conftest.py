import pathlib

import pytest

HERE = pathlib.Path(__file__).parent


@pytest.fixture(scope="session")
def bufr_test_dir():
    """Directory where real BUFR test files live (optional)."""
    return HERE / "data" / "bufr"


@pytest.fixture
def sample_sweep_bytes():
    """Return a small synthetic sweep bytes compressed with zlib."""
    import zlib

    import numpy as np

    nrays, ngates = 4, 3
    arr = (np.arange(nrays * ngates, dtype=np.float64) + 0.5).reshape((nrays, ngates))
    raw = arr.tobytes()
    comp = zlib.compress(raw)
    return {
        "nrays": nrays,
        "ngates": ngates,
        "compress_data": comp,
    }
