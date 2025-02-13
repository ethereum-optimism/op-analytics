import itertools
from datetime import date, timedelta

import polars as pl


from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.request import new_session
from op_analytics.coreutils.time import now_date
from op_analytics.coreutils.threads import run_concurrently

from ..dataaccess import DefiLlama
from .protocol import ProtocolTVL
from .metadata import ProtocolMetadata

log = structlog.get_logger()


TVL_TABLE_LAST_N_DAYS = 3


def execute_pull():
    session = new_session()
    process_dt = now_date()

    # Fetch the list of protocols and their metadata
    metadata = ProtocolMetadata.fetch(session, process_dt)
    DefiLlama.PROTOCOLS_METADATA.write(
        dataframe=metadata.df,
        sort_by=["protocol_slug"],
    )

    # Create list of slugs to fetch protocol-specific data
    slugs = metadata.slugs()

    # Find out which slugs are still pending.
    buffered_slugs = get_buffered_slugs(process_dt=process_dt)
    pending_slugs = list(set(slugs) - set(buffered_slugs))

    # Fetch data and write to buffer for pending slugs.
    log.info(f"fetching and buffering data for {len(pending_slugs)}/{len(slugs)} pending slugs")
    run_concurrently(
        function=lambda x: process_batch(session, process_dt, x),
        targets=list(itertools.batched(pending_slugs, n=50)),
        max_workers=5,
    )
    log.info("done fetching and buffering data")

    # Write data for the last N dates to GCS.
    DefiLlama.PROTOCOLS_TVL.write_gcs_from_clickhouse_buffer(
        process_dt=process_dt,
        min_dt=process_dt - timedelta(days=TVL_TABLE_LAST_N_DAYS),
        max_dt=process_dt,
        sort_by=["protocol_slug", "chain"],
    )
    DefiLlama.PROTOCOLS_TOKEN_TVL.write_gcs_from_clickhouse_buffer(
        process_dt=process_dt,
        min_dt=process_dt - timedelta(days=TVL_TABLE_LAST_N_DAYS),
        max_dt=process_dt,
        sort_by=["protocol_slug", "chain", "token"],
    )


def process_batch(session, process_dt, batch: list[str]):
    """Process a batch of slugs.

    Fetch data for all slugs in the batch and write to the ingestion buffer in ClickHouse."""
    protocols: list[ProtocolTVL] = []
    for slug in batch:
        protocols.append(ProtocolTVL.fetch(session, slug))

    tvl_df = pl.concat(_.tvl_df for _ in protocols)
    token_tvl_df = pl.concat(_.token_tvl_df for _ in protocols)

    DefiLlama.PROTOCOLS_TVL.write_to_clickhouse_buffer(
        tvl_df.with_columns(process_dt=pl.lit(process_dt))
    )
    DefiLlama.PROTOCOLS_TOKEN_TVL.write_to_clickhouse_buffer(
        token_tvl_df.with_columns(process_dt=pl.lit(process_dt))
    )


def get_buffered_slugs(process_dt: date):
    """Slugs that have been processed before.

    Find which slugs have already been written to the ingestion buffer in ClickHouse."""
    slugs1: set[str] = DefiLlama.PROTOCOLS_TVL.query_clickhouse_buffer_key(
        key_column="protocol_slug",
        process_dt=process_dt,
    )

    slugs2: set[str] = DefiLlama.PROTOCOLS_TOKEN_TVL.query_clickhouse_buffer_key(
        key_column="protocol_slug",
        process_dt=process_dt,
    )
    return slugs1.intersection(slugs2)
