from datetime import timedelta

from op_coreutils import clickhouse
from op_coreutils.logger import structlog
from op_coreutils.partitioned import SinkMarkerPath
from op_coreutils.time import datetime_fromepoch, now_seconds

from .batches import BlockBatch
from .sinks import RawOnchainDataSink
from .utilities import RawOnchainDataLocation, RawOnchainDataProvider

log = structlog.get_logger()


def all_outputs_complete(
    sinks: list[RawOnchainDataLocation], markers: list[SinkMarkerPath]
) -> bool:
    """Check if all outputs are complete.

    This function is somewhat low-level in that it receives the explicit completion
    markers that we are looking for. It checks that those markers are present in all
    of the data sinks.
    """
    result = True
    for data_location in sinks:
        sink = RawOnchainDataSink(location=data_location)
        complete = []
        incomplete = []
        for marker in markers:
            if sink.is_complete(marker):
                complete.append(marker)
            else:
                incomplete.append(marker)

        log.info(
            f"{len(complete)} complete, {len(incomplete)} incomplete on sink={sink.location.name}"
        )

        if incomplete:
            log.info(f"Showing the first 5 incomplete locations at {sink.location.name}")
            for marker_location in sorted(incomplete)[:5]:
                log.info(f"DataSink {sink.location.name!r} is incomplete at {marker_location!r}")
            result = False

    return result


def all_inputs_ready(provider: RawOnchainDataProvider, block_batch: BlockBatch) -> bool:
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
        block_batch=block_batch,
        chain_max_block=max_block,
        chain_max_ts=max_ts,
    )


SAFE_BLOCK_LAG = 1000

SAFE_PROVIDER_SLA = timedelta(hours=3)


def is_safe(block_batch: BlockBatch, chain_max_block: int, chain_max_ts: int):
    """Check if the block batch is safe to process.

    Reasons for not being safe:

    - Block number is too close to the tip of the chain.
    - The data provider is lagging to far from real time


    """
    diff = chain_max_block - block_batch.max
    if diff < SAFE_BLOCK_LAG:
        log.warning(
            "Batch is not safe to process: too close to max block",
            chain_max=f"#{chain_max_block}",
            diff=diff,
        )
        return False

    chain_max = datetime_fromepoch(chain_max_ts)
    current_time = now_seconds()
    ts_diff = current_time - chain_max
    ts_diff_hours = round(ts_diff.total_seconds() / 3600.0, 1)

    if ts_diff > SAFE_PROVIDER_SLA:
        log.warning(
            "Batch is not safe to process: provider may be significantly lagging behind",
            provider_max_ts=chain_max.isoformat(),
            current_ts=current_time.isoformat(),
            diff=f"{ts_diff_hours}hrs",
        )
        return False
    return True
