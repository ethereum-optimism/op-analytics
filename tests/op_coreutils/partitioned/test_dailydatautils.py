import datetime

import polars as pl

from op_analytics.coreutils.partitioned.dailydatautils import last_n_dts, dt_summary


def test_last_n_dts():
    actual = last_n_dts(n_dates=3, reference_dt="2025-01-11")
    assert actual == [
        datetime.date(2025, 1, 9),
        datetime.date(2025, 1, 10),
        datetime.date(2025, 1, 11),
    ]


def test_df_summary():
    df = pl.DataFrame(
        [
            {"dt": "2025-01-01", "val": None},
            {"dt": "2025-01-01", "val": None},
            {"dt": "2025-01-01", "val": None},
            {"dt": "2025-01-02", "val": None},
            {"dt": "2025-01-02", "val": None},
            {"dt": "2025-01-03", "val": None},
        ]
    )
    assert dt_summary(df) == {
        "total": 6,
        "dts": {
            "2025-01-03": 1,
            "2025-01-02": 2,
            "2025-01-01": 3,
        },
    }


def test_df_summary_no_dt():
    df = pl.DataFrame(
        [
            {"dtblah": "2025-01-01", "val": None},
            {"dtblah": "2025-01-01", "val": None},
            {"dtblah": "2025-01-01", "val": None},
            {"dtblah": "2025-01-02", "val": None},
            {"dtblah": "2025-01-02", "val": None},
            {"dtblah": "2025-01-03", "val": None},
        ]
    )
    assert dt_summary(df) == {"total": 6, "dts": None}
