import datetime

from op_analytics.coreutils.partitioned.dailydata import last_n_dts


def test_last_n_dts():
    actual = last_n_dts(n_dates=3, reference_dt="2025-01-11")
    assert actual == [
        datetime.date(2025, 1, 9),
        datetime.date(2025, 1, 10),
        datetime.date(2025, 1, 11),
    ]
