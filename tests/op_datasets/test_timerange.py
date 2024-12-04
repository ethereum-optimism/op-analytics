import datetime
from unittest.mock import patch

from op_analytics.coreutils.rangeutils.timerange import TimeRange


def test_daterange_01():
    dr = TimeRange.from_spec("@20241001:20241030")
    assert dr == TimeRange(
        min=datetime.datetime(2024, 10, 1),
        max=datetime.datetime(2024, 10, 30),
        max_requested_timestamp=1730246400,
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
        max_requested_timestamp=None,
    )
