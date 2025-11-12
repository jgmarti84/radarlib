import numpy as np
import pandas as pd

from radarlib.io.bufr import pyart_writer as pw


def make_field(filename: str, ngates: int, nrays: int, gate_offset: int, gate_size: int, elev: float):
    sweeps = pd.DataFrame(
        {
            "ngates": [ngates],
            "nrayos": [nrays],
            "gate_offset": [gate_offset],
            "gate_size": [gate_size],
            "elevaciones": [elev],
        }
    )
    info = {"sweeps": sweeps, "metadata": {"instrument_name": "TEST_RADAR"}, "filename": filename, "nsweeps": 1}
    data = np.ma.ones((nrays, ngates), dtype=np.float32) * 5.0
    return {"info": info, "data": data, "tipo_producto": filename.split("_")[2]}


def test_find_reference_field():
    f1 = make_field("R_1_DBZH_1.BUFR", 100, 10, 0, 100, 1.0)
    f2 = make_field("R_1_VRAD_1.BUFR", 150, 10, 0, 100, 1.0)
    idx = pw._find_reference_field([f1, f2])
    assert idx == 1


def test_align_field_to_reference():
    # reference 200 gates, field has offset 100 and 50 gates
    ref_ngates = 200
    field = make_field("R_1_DBZH_1.BUFR", 50, 5, 100, 100, 1.0)
    aligned = pw._align_field_to_reference(field, ref_gate_offset=0, ref_gate_size=100, ref_ngates=ref_ngates)
    assert aligned["data"].shape == (5, ref_ngates)
    # check that values were placed starting at gate 1 (index 1 because offset 100 / gate_size 100)
    assert aligned["data"][0, 1] == 5.0
