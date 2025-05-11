import itertools
from dataclasses import dataclass
from datetime import date

import polars as pl

from op_analytics.coreutils.logger import memory_usage, structlog
from op_analytics.coreutils.partitioned.dailydata import TablePath
from op_analytics.coreutils.request import new_session
from op_analytics.coreutils.subprocs import run_in_subprocesses

log = structlog.get_logger()


def write_to_buffer(pending_ids: list[str], process_dt: date):
    """Pull data from DefiLlama and write to the ClickHouse buffer."""

    # Split into batches of "batch_size" slugs per batch.
    batch_size = 40
    num_pending = len(pending_ids)
    total_tasks = (num_pending // batch_size) + (1 if num_pending % batch_size > 0 else 0)

    # Loop over batches in subprocess to avoid memory usage creep. The subprocesses
    # return memory back to the os.
    run_in_subprocesses(
        target=fetch_and_write,
        tasks=batches(
            process_dt=process_dt,
            pending_ids=pending_ids,
            n=batch_size,
        ),
        total_tasks=total_tasks,
        fork=True,  # Set to False when debugging locally
    )

    log.info("done fetching and buffering data")


def batches(process_dt: date, pending_ids: list[str], n: int):
    """Split the pending ids into batches for processing."""

    for batch in itertools.batched(pending_ids, n=n):
        yield Batch(
            process_dt=process_dt,
            slugs=list(batch),
        )


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
