from datetime import datetime
from op_coreutils.time import dt_fromepoch, datetime_fromepoch


def test_epoch_dt():
    actual = dt_fromepoch(0)
    assert actual == "1970-01-01"


def test_epoch_datetime():
    actual = datetime_fromepoch(0)
    assert actual == datetime(1970, 1, 1)


def test_epoch_dt_01():
    actual = dt_fromepoch(1729797569)
    assert actual == "2024-10-24"


def test_epoch_datetime_01():
    actual = datetime_fromepoch(1729797569)
    assert actual == datetime(2024, 10, 24, 19, 19, 29)
