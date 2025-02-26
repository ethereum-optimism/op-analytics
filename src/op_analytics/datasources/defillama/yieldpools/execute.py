import itertools
from datetime import date


from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.request import new_session
from op_analytics.coreutils.threads import run_concurrently
from op_analytics.coreutils.time import now_date

from op_analytics.datasources.defillama.dataaccess import DefiLlama


from .metadata import YieldPoolsMetadata
from .utils import get_buffered, fetch_and_write, copy_to_gcs

log = structlog.get_logger()


YIELD_TABLE_LAST_N_DAYS = 360


def execute_pull(process_dt: date | None = None):
    """Daily pull yield pool data from DefiLlama."""

    session = new_session()
    process_dt = process_dt or now_date()

    # Fetch the list of pools and their metadata
    metadata = YieldPoolsMetadata.fetch(session=session, process_dt=process_dt)
    DefiLlama.YIELD_POOLS_METADATA.write(
        dataframe=metadata.df,
        sort_by=["pool"],
    )

    # Get pool IDs to process
    pool_ids = metadata.pool_ids()

    # Fetch from API And write to buffer.
    write_to_buffer(
        session=session,
        pools=pool_ids,
        process_dt=process_dt,
        metadata=metadata,
    )

    # Copy data from buffer to GCS.
    return copy_to_gcs(process_dt=process_dt, last_n_days=YIELD_TABLE_LAST_N_DAYS)


def write_to_buffer(
    session,
    pools: list[str],
    process_dt: date,
    metadata: YieldPoolsMetadata,
):
    """Pull data from DefiLlama and write to the ClickHouse buffer.

    This part was split up as a a separate function just so it is easy to add retries
    to this part of the process only if we need to later on.
    """

    # Find out which ids are still pending.
    buffered_ids = get_buffered(process_dt=process_dt)
    pending_ids = list(set(pools) - set(buffered_ids))

    # Ucomment to limit the number of pools we fetch from. Useful for debugging.
    # pending_pools = pending_pools[:10]

    # Fetch data and write to buffer for pending yield pools.
    log.info(f"fetching and buffering data for {len(pending_ids)}/{len(pools)} pending pools")
    run_concurrently(
        function=lambda x: fetch_and_write(
            session=session,
            process_dt=process_dt,
            batch=x,
            metadata=metadata,
        ),
        targets=list(itertools.batched(pending_ids, n=20)),
        max_workers=4,
    )
    log.info("done fetching and buffering data")
