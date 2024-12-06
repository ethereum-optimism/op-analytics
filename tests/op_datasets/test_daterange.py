import datetime
from unittest.mock import patch

from op_analytics.coreutils.rangeutils.daterange import DateRange


def test_daterange():
    dr = DateRange.from_spec("@20241001:20241030")
    assert dr == DateRange(
        min=datetime.date(2024, 10, 1),
        max=datetime.date(2024, 10, 30),
        max_requested_timestamp=1730246400,
    )
    assert len(dr) == 29


def test_blockrange_plus():
    dr = DateRange.from_spec("@20241001:+30")
    assert dr == DateRange(
        min=datetime.date(2024, 10, 1),
        max=datetime.date(2024, 10, 31),
        max_requested_timestamp=1730332800,
    )
    assert len(dr) == 30


@patch("op_analytics.coreutils.rangeutils.daterange.now_date", lambda: datetime.date(2024, 11, 17))
def test_minus_days():
    dr = DateRange.from_spec("m3days")  # 3 days from the current date
    assert dr == DateRange(
        min=datetime.date(2024, 11, 15),
        max=datetime.date(2024, 11, 18),
        max_requested_timestamp=None,
    )
    assert dr.dates() == [
        datetime.date(2024, 11, 15),
        datetime.date(2024, 11, 16),
        datetime.date(2024, 11, 17),
    ]
    assert len(dr) == 3


def test_epoch():
    dr = DateRange.from_spec("@20241001:+30")
    assert dr == DateRange(
        min=datetime.date(2024, 10, 1),
        max=datetime.date(2024, 10, 31),
        max_requested_timestamp=1730332800,
    )
    assert dr.min_ts == 1727740800
    assert dr.max_ts == 1730332800
