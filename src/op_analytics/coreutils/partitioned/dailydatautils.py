from datetime import date, timedelta

import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.rangeutils.daterange import DateRange
from op_analytics.coreutils.time import date_fromstr, date_tostr


log = structlog.get_logger()


def last_n_dts(n_dates: int, reference_dt: str) -> list[date]:
    """Produce a list of N dates starting from reference_dt.

    The reference_dt will be included in the list results.
    """
    max_date = date_fromstr(reference_dt) + timedelta(days=1)
    return DateRange(
        min=max_date - timedelta(days=n_dates),
        max=max_date,
        requested_max_timestamp=None,
    ).dates()


def last_n_days(
    df: pl.DataFrame,
    n_dates: int,
    reference_dt: str,
    date_column: str = "dt",
    date_column_type_is_str: bool = False,
) -> pl.DataFrame:
    """Limit dataframe to the last N dates present in the data.

    This function is helpful when doing a dynamic partition overwrite. It allows us
    to select only recent partitions to update.

    Usually operates on partitioned datasets so the default date_column is "dt". If
    needed for other purposes callers can specify a different date_column name.
    """
    dts: list[date] = last_n_dts(n_dates=n_dates, reference_dt=reference_dt)

    if date_column_type_is_str:
        dts_str = [date_tostr(_) for _ in dts]
        return df.filter(pl.col(date_column).is_in(dts_str))

    return df.filter(pl.col(date_column).is_in(dts))


def dt_summary(df: pl.DataFrame) -> dict:
    """Return a summary of the dataframe broken down by "dt"."""

    if "dt" in df:
        dt_counts = {
            row["dt"]: row["len"]
            for row in df.group_by("dt").len().sort("dt", descending=True).to_dicts()
        }
    else:
        dt_counts = None

    return {
        "total": len(df),
        "dts": dt_counts,
    }
