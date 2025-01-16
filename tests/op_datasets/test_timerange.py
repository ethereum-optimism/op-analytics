import datetime
from unittest.mock import patch

from op_analytics.coreutils.rangeutils.daterange import DateRange
from op_analytics.coreutils.rangeutils.timerange import TimeRange


def test_daterange_01():
    dr = TimeRange.from_spec("@20241001:20241030")
    assert dr == TimeRange(
        min=datetime.datetime(2024, 10, 1),
        max=datetime.datetime(2024, 10, 30),
        requested_max_timestamp=1730246400,
    )


@patch(
    "op_analytics.coreutils.rangeutils.timerange.now",
    lambda: datetime.datetime(2024, 11, 17, 5, 30),
)
def test_daterange_02():
    dr = TimeRange.from_spec("m12hours")
    assert dr == TimeRange(
        min=datetime.datetime(2024, 11, 16, 17, 30),
        max=datetime.datetime(2024, 11, 17, 5, 30),
        requested_max_timestamp=None,
    )


def test_to_daterange_01():
    tr = TimeRange.from_spec("@20241001:20241030")
    assert tr == TimeRange(
        min=datetime.datetime(2024, 10, 1),
        max=datetime.datetime(2024, 10, 30),
        requested_max_timestamp=1730246400,
    )

    dr = tr.to_date_range()
    assert dr == DateRange(
        min=datetime.date(2024, 10, 1),
        max=datetime.date(2024, 10, 30),
        requested_max_timestamp=1730246400,
    )


def test_to_daterange_02():
    max_dt = datetime.datetime(2024, 10, 28, 23, 45, tzinfo=datetime.timezone.utc)
    tr = TimeRange(
        min=datetime.datetime(2024, 10, 1, 12, 50),
        max=datetime.datetime(2024, 10, 28, 23, 45),
        requested_max_timestamp=int(max_dt.timestamp()),
    )

    dr = tr.to_date_range()
    assert dr == DateRange(
        min=datetime.date(2024, 10, 1),
        max=datetime.date(2024, 10, 29),
        requested_max_timestamp=1730159100,
    )

    assert dr.dates() == [
        datetime.date(2024, 10, 1),
        datetime.date(2024, 10, 2),
        datetime.date(2024, 10, 3),
        datetime.date(2024, 10, 4),
        datetime.date(2024, 10, 5),
        datetime.date(2024, 10, 6),
        datetime.date(2024, 10, 7),
        datetime.date(2024, 10, 8),
        datetime.date(2024, 10, 9),
        datetime.date(2024, 10, 10),
        datetime.date(2024, 10, 11),
        datetime.date(2024, 10, 12),
        datetime.date(2024, 10, 13),
        datetime.date(2024, 10, 14),
        datetime.date(2024, 10, 15),
        datetime.date(2024, 10, 16),
        datetime.date(2024, 10, 17),
        datetime.date(2024, 10, 18),
        datetime.date(2024, 10, 19),
        datetime.date(2024, 10, 20),
        datetime.date(2024, 10, 21),
        datetime.date(2024, 10, 22),
        datetime.date(2024, 10, 23),
        datetime.date(2024, 10, 24),
        datetime.date(2024, 10, 25),
        datetime.date(2024, 10, 26),
        datetime.date(2024, 10, 27),
        datetime.date(2024, 10, 28),
    ]
