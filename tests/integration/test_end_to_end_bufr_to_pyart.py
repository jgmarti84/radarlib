from pathlib import Path

import pytest


@pytest.mark.integration
@pytest.mark.filterwarnings("ignore:numpy.ndarray size changed.*:RuntimeWarning")
def test_end_to_end_bufr_to_pyart(tmp_path: Path):
    # Locate sample BUFR files under tests/data/bufr
    data_dir = Path("tests/data/bufr")
    if not data_dir.exists():
        pytest.skip("No BUFR test data available")

    bufr_files = list(data_dir.glob("*.BUFR"))
    if not bufr_files:
        pytest.skip("No BUFR test files found")

    # Also skip if dynamic library missing
    # lib_path = Path("src/radarlib/io/bufr/bufr_resources/dynamic_library/libdecbufr.so")
    # if not lib_path.exists():
    #     pytest.skip("C decBUFR library not available in test environment")

    from radarlib.io.bufr.pyart_writer import bufr_paths_to_pyart

    results = bufr_paths_to_pyart([str(bufr_files[0])], root_resources=None, save_path=tmp_path)
    assert results
    p, radar = results[0]
    assert radar is not None
    # output file should exist
    out_file = tmp_path / f"{Path(p).stem}.nc"
    assert out_file.exists()
