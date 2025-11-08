import pytest
from radarlib.core import YourClass, your_function


def test_your_function_adds_numbers():
    assert your_function(2, 3) == 5
    assert your_function(2.5, 0.5) == pytest.approx(3.0)


def test_yourclass_initial_state_and_add():
    obj = YourClass()
    assert obj.mean() is None
    assert obj.max() is None

    obj.add(10)
    obj.add(20)
    assert obj.mean() == pytest.approx(15.0)
    assert obj.max() == 20


def test_yourclass_init_with_samples_and_clear():
    obj = YourClass([1, 2, 3])
    assert obj.mean() == pytest.approx(2.0)
    assert obj.max() == 3

    obj.clear()
    assert obj.mean() is None
    assert obj.max() is None