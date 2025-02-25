from datetime import date, timedelta

import polars as pl

from op_analytics.coreutils.clickhouse.oplabs import insert_oplabs, run_query_oplabs
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydata import TablePath
from op_analytics.coreutils.partitioned.dailydatawritefromclickhouse import FromClickHouseWriter


from ..dataaccess import DefiLlama
from .protocol import ProtocolTVL

log = structlog.get_logger()


def get_buffered(process_dt: date):
    """Slugs that have been processed before.

    Find which slugs have already been written to the ingestion buffer in ClickHouse."""

    slugs1: set[str] = _query_slugs(
        table=DefiLlama.PROTOCOLS_TVL.clickhouse_buffer_table(), process_dt=process_dt
    )

    slugs2: set[str] = _query_slugs(
        table=DefiLlama.PROTOCOLS_TOKEN_TVL.clickhouse_buffer_table(), process_dt=process_dt
    )

    return slugs1.intersection(slugs2)


def fetch_and_write(session, process_dt, batch: list[str]):
    """Fetch data and write to the ingestion buffer in ClickHouse."""

    protocols: list[ProtocolTVL] = []
    for slug in batch:
        protocols.append(ProtocolTVL.fetch(session, slug))

    tvl_df = pl.concat(_.tvl_df for _ in protocols)
    token_tvl_df = pl.concat(_.token_tvl_df for _ in protocols)

    _write_buffer(
        table=DefiLlama.PROTOCOLS_TVL.clickhouse_buffer_table(),
        df=tvl_df.with_columns(process_dt=pl.lit(process_dt)),
    )

    _write_buffer(
        table=DefiLlama.PROTOCOLS_TOKEN_TVL.clickhouse_buffer_table(),
        df=token_tvl_df.with_columns(process_dt=pl.lit(process_dt)),
    )


def copy_to_gcs(process_dt: date, last_n_days: int):
    """Write data for the last N dates to GCS."""

    min_dt = process_dt - timedelta(days=last_n_days)

    results = []

    writer1 = FromClickHouseWriter(
        dailydata_table=DefiLlama.PROTOCOLS_TVL,
        process_dt=process_dt,
        min_dt=min_dt,
        max_dt=process_dt,
        order_by="protocol_slug, chain",
    )
    results.append(writer1.write().to_dict())

    writer2 = FromClickHouseWriter(
        dailydata_table=DefiLlama.PROTOCOLS_TOKEN_TVL,
        process_dt=process_dt,
        min_dt=min_dt,
        max_dt=process_dt,
        order_by="protocol_slug, chain, token",
    )
    results.append(writer2.write().to_dict())

    return results


def _query_slugs(table: TablePath, process_dt: date):
    """Query distinct slugs on ClickhHouse."""

    return set(
        run_query_oplabs(
            query=f"""
            SELECT DISTINCT protocol_slug
            FROM {table.db}.{table.table} FINAL
            WHERE process_dt = {{param1:Date}}
            """,
            parameters={"param1": process_dt},
        )["protocol_slug"].to_list()
    )


def _write_buffer(table: TablePath, df: pl.DataFrame) -> None:
    result = insert_oplabs(
        database=table.db,
        table=table.table,
        df_arrow=df.to_arrow(),
    )
    log.info(f"inserted {result.written_rows} rows to {table.db}.{table.table}")
