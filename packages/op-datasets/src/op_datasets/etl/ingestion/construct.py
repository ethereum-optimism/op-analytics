from op_coreutils import clickhouse
from op_coreutils.logger import structlog
from op_coreutils.partitioned import DataLocation
from op_coreutils.threads import run_concurrently

from op_datasets.utils.blockrange import BlockRange
from op_datasets.utils.daterange import DateRange

from .batches import BlockBatch, split_block_range
from .sources import RawOnchainDataProvider
from .task import IngestionTask

log = structlog.get_logger()


def construct_tasks(
    chains: list[str],
    range_spec: str,
    read_from: RawOnchainDataProvider,
    write_to: list[DataLocation],
):
    blocks_by_chain: dict[str, BlockRange]

    try:
        block_range = BlockRange.from_spec(range_spec)
        blocks_by_chain = {}
        for chain in chains:
            blocks_by_chain[chain] = block_range
        max_requested_timestamp = None

    except NotImplementedError:
        # Ensure range_spec is a valid DateRange.
        date_range = DateRange.from_spec(range_spec)

        def blocks_for_chain(ch):
            return block_range_for_dates(chain=ch, date_spec=range_spec)

        blocks_by_chain = run_concurrently(blocks_for_chain, targets=chains, max_workers=4)
        max_requested_timestamp = date_range.max_ts

    # Batches to be ingested for each chain.
    chain_batches: dict[str, list[BlockBatch]] = {}
    for chain, chain_block_range in blocks_by_chain.items():
        chain_batches[chain] = split_block_range(chain, chain_block_range)

    # Log a summary of the work that will be done for each chain.
    for chain, batches in chain_batches.items():
        if batches:
            total_blocks = batches[-1].max - batches[0].min
            log.info(
                f"Prepared chain={chain!r}: {len(batches)} batch(es) {total_blocks} total blocks starting at #{batches[0].min}"
            )
        else:
            log.info(f"Prepared chain={chain!r}: {len(batches)} batch(es)")

    # Collect a single list of tasks to perform across all chains.
    all_tasks: list[IngestionTask] = []
    for batches in chain_batches.values():
        for batch in batches:
            all_tasks.append(
                IngestionTask.new(
                    max_requested_timestamp=max_requested_timestamp,
                    block_batch=batch,
                    read_from=read_from,
                    write_to=write_to,
                )
            )

    return all_tasks


def block_range_for_dates(chain: str, date_spec: str):
    """Find the block range required to cover the provided dates.

    Uses the raw blocks dataset in Goldsky Clickhouse to find out which blocks have
    timestamps in the required dates.

    Onchain tables are sorted by block_number and not by timestamp. When backfilling the
    ingestion process it is useful to know what is the range of blocks that spans a given
    date.

    In this way we can run ingestion by date instead of by block number, which makes it
    easier to generalize the process across chains.
    """
    date_range = DateRange.from_spec(date_spec)

    # Not an f-string to preserve the curly brackets for query params
    where = "timestamp >= {mints:UInt64} AND timestamp < {maxts:UInt64}"

    result = clickhouse.run_goldsky_query(
        query=f"""
        SELECT
            min(number) as block_min,
            max(number) as block_max
        FROM {chain}_blocks
        WHERE {where}
        """,
        parameters={
            "mints": date_range.min_ts,
            "maxts": date_range.max_ts,
        },
    )

    assert len(result) == 1
    row = result.to_dicts()[0]
    return BlockRange(row["block_min"], row["block_max"])
