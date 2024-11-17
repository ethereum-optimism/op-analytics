from datetime import timedelta

from op_analytics.coreutils import clickhouse
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.time import datetime_fromepoch, now_trunc

from .batches import BlockBatch
from .sources import RawOnchainDataProvider

log = structlog.get_logger()


def all_inputs_ready(
    provider: RawOnchainDataProvider,
    block_batch: BlockBatch,
    max_requested_timestamp: int | None,
) -> bool:
    """Very that a block batch is safe to ingest.

    We don't want to ingest data that is too close to the tip of the chain
    because it might not yet be immutable.

    We hard-code a 1hr buffer on the most recently seen block.
    """
    if provider != RawOnchainDataProvider.GOLDSKY:
        raise ValueError(f"only goldsky is a suppported provider: {provider}")

    result = clickhouse.run_goldsky_query(
        query=f"""
        SELECT
            max(number) as block_max,
            max(timestamp) as timestamp_max
        FROM {block_batch.chain}_blocks
        """
    )

    assert len(result) == 1
    row = result.to_dicts()[0]

    max_ts: int = row["timestamp_max"]
    max_block: int = row["block_max"]

    return is_safe(
        max_requested_timestamp=max_requested_timestamp,
        block_batch=block_batch,
        chain_max_block=max_block,
        chain_max_ts=max_ts,
    )


SAFE_BLOCK_LAG = 1000

SAFE_PROVIDER_SLA = timedelta(hours=3)


def is_safe(
    max_requested_timestamp: int | None,
    block_batch: BlockBatch,
    chain_max_block: int,
    chain_max_ts: int,
):
    """Check if the block batch is safe to process.

    Reasons for not being safe:

    - Block number is too close to the tip of the chain.
    - The data provider is lagging to far from real time


    """
    diff = chain_max_block - block_batch.max
    if diff < SAFE_BLOCK_LAG:
        log.warning(
            f"skipping unsafe batch: too close to max block: chain {chain_max_block} is {diff} ahead"
        )
        return False

    chain_max = datetime_fromepoch(chain_max_ts)
    if max_requested_timestamp is not None:
        requested_time = datetime_fromepoch(max_requested_timestamp)
    else:
        requested_time = now_trunc()

    ts_diff = requested_time - chain_max
    ts_diff_hours = round(ts_diff.total_seconds() / 3600.0, 1)
    diffstr = f"{ts_diff_hours}hrs"

    if ts_diff > SAFE_PROVIDER_SLA:
        log.warning(
            f"skipping unsafe batch: provider lag: {diffstr}",
            provider_max_ts=chain_max.isoformat(),
            requested_time=requested_time.isoformat(),
            diff=diffstr,
        )
        return False
    return True
