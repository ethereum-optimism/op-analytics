import pytest

from op_analytics.transforms.updates import StepType


def test_step_type():
    assert StepType("dim") == StepType.DIM


def test_step_type_invalid():
    with pytest.raises(ValueError) as ex:
        StepType("bla")
    assert ex.value.args == ("'bla' is not a valid StepType",)
