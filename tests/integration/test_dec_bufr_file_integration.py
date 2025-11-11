import pytest

from radarlib.io.bufr import bufr as bufr_mod


@pytest.mark.integration
def test_dec_bufr_with_real_file(bufr_test_dir):
    files = list(bufr_test_dir.glob("*.BUFR"))
    if not files:
        pytest.skip("No BUFR files available in tests/data/bufr")
    f = str(files[0])
    out = bufr_mod.bufr_to_dict(f, logger_name="integration_test")
    assert out is not None
    assert "data" in out and "info" in out
