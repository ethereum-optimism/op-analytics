import itertools
from datetime import date


from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.request import new_session
from op_analytics.coreutils.threads import run_concurrently
from op_analytics.coreutils.time import now_date

from ..dataaccess import DefiLlama
from .metadata import ProtocolMetadata
from .utils import copy_to_gcs, fetch_and_write_slugs, get_buffered_slugs

log = structlog.get_logger()


TVL_TABLE_LAST_N_DAYS = 360


def execute_pull(process_dt: date | None = None):
    """Daily pull protocol TVL data from DefiLlama."""

    session = new_session()
    process_dt = process_dt or now_date()

    # Fetch the list of protocols and their metadata
    metadata = ProtocolMetadata.fetch(session, process_dt)
    DefiLlama.PROTOCOLS_METADATA.write(
        dataframe=metadata.df,
        sort_by=["protocol_slug"],
    )

    # Create list of slugs to fetch protocol-specific data
    slugs = metadata.slugs()

    # Fetch from API And write to buffer.
    write_to_buffer(session, slugs, process_dt)

    # Copy data from buffer to GCS.
    return copy_to_gcs(process_dt=process_dt, last_n_days=TVL_TABLE_LAST_N_DAYS)


def write_to_buffer(session, slugs: list[str], process_dt: date):
    """Pull data from DefiLlama and write to the ClickHouse buffer.

    This part was split up as a a separate function just so it is easy to add retries
    to this part of the process only if we need to later on.
    """

    # Find out which slugs are still pending.
    buffered_slugs = get_buffered_slugs(process_dt=process_dt)
    pending_slugs = list(set(slugs) - set(buffered_slugs))

    # Fetch data and write to buffer for pending slugs.
    log.info(f"fetching and buffering data for {len(pending_slugs)}/{len(slugs)} pending slugs")
    run_concurrently(
        function=lambda x: fetch_and_write_slugs(session, process_dt, x),
        targets=list(itertools.batched(pending_slugs, n=20)),
        max_workers=8,
    )
    log.info("done fetching and buffering data")
