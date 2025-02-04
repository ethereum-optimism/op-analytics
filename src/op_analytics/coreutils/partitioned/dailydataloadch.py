from datetime import timedelta

import polars as pl

from op_analytics.coreutils.clickhouse.oplabs import (
    insert_oplabs,
    run_query_oplabs,
)
from op_analytics.coreutils.logger import human_rows, human_size, structlog


log = structlog.get_logger()


def load_to_clickhouse(
    db: str,
    table: str,
    dataframe: pl.DataFrame,
    min_date: str | None = None,
    max_date: str | None = None,
    incremental_overlap: int = 0,
):
    """Incrementally load daily data from GCS to clickhouse.

    When date ranges are not provided this function queries clickhouse to find the latest loaded
    date and proceeds from there.

    When a backfill is needed you can provide min_date/max_date accordingly.

    When incremental_overlap is provided, the function will reload "dt" values that already exist
    in clickhouse. This is useful when the data for recent dates is still changing at the source.
    It allows us to reload with the latest most valid version.
    """

    used_clickhouse_watermark = False
    if min_date is None:
        clickhouse_watermark = (
            run_query_oplabs(f"SELECT max(dt) as max_dt FROM {db}.{table}")
            .with_columns(max_dt=pl.from_epoch(pl.col("max_dt"), time_unit="d"))
            .item()
        )
        min_date = (
            clickhouse_watermark + timedelta(days=1) - timedelta(days=incremental_overlap)
        ).strftime("%Y-%m-%d")
        used_clickhouse_watermark = True

    log.info(
        "incremental load to clickhouse",
        min_date=min_date,
        max_date=max_date,
        used_clickhouse_watermark=used_clickhouse_watermark,
    )

    query_summary = insert_oplabs(database=db, table=table, df_arrow=dataframe.to_arrow())

    summary_dict = dict(
        written_bytes=human_size(query_summary.written_bytes()),
        written_rows=human_rows(query_summary.written_rows),
    )

    log.info("insert summary", **summary_dict)
    return summary_dict
