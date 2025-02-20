from datetime import date, timedelta

import polars as pl

from op_analytics.coreutils.clickhouse.oplabs import insert_oplabs, run_query_oplabs
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydatawritefromclickhouse import FromClickHouseWriter


from ..dataaccess import DefiLlama
from .metadata import YieldPoolsMetadata
from .yieldpool import YieldPool

log = structlog.get_logger()


def get_buffered(process_dt: date):
    """Pools that have been processed before.

    Find which pools have already been written to the ingestion buffer in ClickHouse."""
    buffer_table = DefiLlama.YIELD_POOLS_HISTORICAL.clickhouse_buffer_table()
    return set(
        run_query_oplabs(
            query=f"""
            SELECT DISTINCT pool
            FROM {buffer_table.db}.{buffer_table.table} FINAL
            WHERE process_dt = {{param1:Date}}
            """,
            parameters={"param1": process_dt},
        )["pool"].to_list()
    )


def fetch_and_write(session, process_dt, batch: list[str], metadata: YieldPoolsMetadata):
    """Fetch data and write to the ingestion buffer in ClickHouse."""

    pools: list[YieldPool] = []
    for pool in batch:
        pools.append(YieldPool.fetch(session, pool, pools_metadata=metadata))
    log.info(f"fetched data for a batch of {len(batch)} yield pools")

    pools_df = pl.concat(_.df for _ in pools).with_columns(process_dt=pl.lit(process_dt))
    buffer_table = DefiLlama.YIELD_POOLS_HISTORICAL.clickhouse_buffer_table()

    result = insert_oplabs(
        database=buffer_table.db,
        table=buffer_table.table,
        df_arrow=pools_df.to_arrow(),
    )
    log.info(f"inserted {result.written_rows} rows to {buffer_table.db}.{buffer_table.table}")


def copy_to_gcs(process_dt: date, last_n_days: int):
    """Write data for the last N dates to GCS."""

    min_dt = process_dt - timedelta(days=last_n_days)

    results = []

    writer1 = FromClickHouseWriter(
        dailydata_table=DefiLlama.YIELD_POOLS_HISTORICAL,
        process_dt=process_dt,
        min_dt=min_dt,
        max_dt=process_dt,
        order_by="pool, protocol_slug, chain",
    )
    results.append(writer1.write().to_dict())

    return results
