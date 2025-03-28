from datetime import timedelta

from op_analytics.coreutils.rangeutils.blockrange import ChainMaxBlock
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.time import datetime_fromepoch, now_trunc

from .sources import RawOnchainDataProvider
from .task import IngestionTask

log = structlog.get_logger()


def all_inputs_ready(task: IngestionTask) -> bool:
    """Very that a block batch is safe to ingest.

    We don't want to ingest data that is too close to the tip of the chain
    because it might not yet be immutable.
    """
    if task.read_from != RawOnchainDataProvider.GOLDSKY:
        raise ValueError(f"only goldsky is a suppported provider: {task.read_from}")

    return is_safe(
        requested_max_timestamp=task.requested_max_timestamp,
        requested_max_block=task.block_batch.max,
        chain_max_block=task.chain_max_block,
    )


SAFE_BLOCK_LAG = 1000

SAFE_PROVIDER_SLA = timedelta(hours=3)


def is_safe(
    requested_max_timestamp: int | None,
    requested_max_block: int,
    chain_max_block: ChainMaxBlock,
):
    """Check if the block batch is safe to process.

    Reasons for not being safe:

    - Block number is too close to the tip of the chain.
    - The data provider is lagging to far from real time


    """

    diff = chain_max_block.number - requested_max_block
    if diff < SAFE_BLOCK_LAG:
        log.warning(
            f"skipping unsafe batch: too close to max block: chain {chain_max_block} is {diff} ahead"
        )
        return False

    chain_max = datetime_fromepoch(chain_max_block.ts)
    if requested_max_timestamp is not None:
        requested_time = datetime_fromepoch(requested_max_timestamp)
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
