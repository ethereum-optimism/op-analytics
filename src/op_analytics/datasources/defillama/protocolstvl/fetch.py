from dataclasses import dataclass
from datetime import date

import polars as pl

from op_analytics.coreutils.clickhouse.oplabs import run_query_oplabs
from op_analytics.coreutils.logger import memory_usage, structlog
from op_analytics.coreutils.partitioned.dailydata import TablePath
from op_analytics.coreutils.request import new_session

from ..dataaccess import DefiLlama

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


@dataclass
class Batch:
    process_dt: date
    slugs: list[str]


def fetch_and_write(batch: Batch):
    """Fetch data and write to the ingestion buffer in ClickHouse.

    This function needs to be pickleable so it can run in a subprocess.
    """
    from op_analytics.coreutils.clickhouse.oplabs import insert_oplabs
    from op_analytics.coreutils.threads import run_concurrently
    from op_analytics.datasources.defillama.dataaccess import DefiLlama as DFL
    from op_analytics.datasources.defillama.protocolstvl.protocol import ProtocolTVL

    session = new_session()
    result: dict[str, ProtocolTVL] = run_concurrently(
        function=lambda x: ProtocolTVL.fetch(slug=x, session=session),
        targets=batch.slugs,
        max_workers=8,
    )
    protocols = list(result.values())

    dtcol = pl.lit(batch.process_dt)
    tvl_df = pl.concat(_.tvl_df for _ in protocols).with_columns(process_dt=dtcol)
    token_tvl_df = pl.concat(_.token_tvl_df for _ in protocols).with_columns(process_dt=dtcol)

    def _write_buffer(table: TablePath, df: pl.DataFrame) -> None:
        result = insert_oplabs(
            database=table.db,
            table=table.table,
            df_arrow=df.to_arrow(),
        )
        log.info(
            f"inserted {result.written_rows} rows to {table.db}.{table.table}",
            max_rss=memory_usage(),
        )

    _write_buffer(table=DFL.PROTOCOLS_TVL.clickhouse_buffer_table(), df=tvl_df)
    _write_buffer(table=DFL.PROTOCOLS_TOKEN_TVL.clickhouse_buffer_table(), df=token_tvl_df)


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
