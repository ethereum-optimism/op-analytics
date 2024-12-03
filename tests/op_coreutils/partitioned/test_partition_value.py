from datetime import date

import pytest

from op_analytics.coreutils.partitioned.partition import PartitionColumn


def test_01():
    with pytest.raises(ValueError) as ex:
        PartitionColumn(name="dt", value=date(2024, 10, 1))  # type: ignore
    assert ex.value.args == ("partition value must be a string: datetime.date(2024, 10, 1)",)


def test_02():
    with pytest.raises(ValueError) as ex:
        PartitionColumn(name="dt", value="2024-00-00")
    assert ex.value.args == ("partition value must be a valid date: '2024-00-00'",)


def test_03():
    with pytest.raises(ValueError) as ex:
        PartitionColumn(name=55, value="2024-01-01")  # type: ignore
    assert ex.value.args == ("partition key must be a string: 55",)


def test_04():
    PartitionColumn(name="dt", value="2024-01-01")


def test_05():
    PartitionColumn(name="chain", value="op")
