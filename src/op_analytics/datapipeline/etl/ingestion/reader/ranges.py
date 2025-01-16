from op_analytics.coreutils.clickhouse.goldsky import run_query_goldsky
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.rangeutils.blockrange import BlockRange, ChainMaxBlock
from op_analytics.coreutils.rangeutils.timerange import TimeRange
from op_analytics.coreutils.time import datetime_fromepoch
from op_analytics.coreutils.threads import run_concurrently


log = structlog.get_logger()


def get_chain_block_ranges(chains: list[str], time_range: TimeRange) -> dict[str, BlockRange]:
    """Query to get block ranges corresponding to the given time range."""

    def blocks_for_chain(ch):
        return block_range_for_dates(
            chain=ch,
            min_ts=time_range.min_ts,
            max_ts=time_range.max_ts,
        )

    return run_concurrently(blocks_for_chain, targets=chains, max_workers=4)


def get_chain_max_blocks(chains: list[str]) -> dict[str, ChainMaxBlock]:
    """Query to get the max block number for each chain."""
    return run_concurrently(chain_max_block, targets=chains, max_workers=4)


def block_range_for_dates(chain: str, min_ts: int, max_ts: int) -> BlockRange:
    """Find the block range required to cover the provided timestamps.

    Uses the raw blocks dataset in Goldsky Clickhouse to find out which blocks have
    timestamps in the required dates.

    Onchain tables are sorted by block_number and not by timestamp. When backfilling the
    ingestion process it is useful to know what is the range of blocks that spans a given
    date.

    In this way we can run ingestion by date instead of by block number, which makes it
    easier to generalize the process across chains.
    """

    # Not an f-string to preserve the curly brackets for query params
    where = "timestamp >= {mints:UInt64} AND timestamp < {maxts:UInt64}"

    result = run_query_goldsky(
        query=f"""
        SELECT
            min(number) as block_min,
            max(number) as block_max
        FROM {chain}_blocks
        WHERE {where}
        """,
        parameters={
            "mints": min_ts,
            "maxts": max_ts,
        },
    )

    assert len(result) == 1
    row = result.to_dicts()[0]
    block_range = BlockRange(row["block_min"], row["block_max"])
    return block_range


def time_range_for_blocks(chain: str, min_block: int, max_block: int) -> TimeRange:
    """Find the time range spanned by the block interval."""

    # Not an f-string to preserve the curly brackets for query params
    where = "number >= {minb:UInt64} AND number < {maxb:UInt64}"

    result = run_query_goldsky(
        query=f"""
        SELECT
            min(timestamp) as time_min,
            max(timestamp) as time_max
        FROM {chain}_blocks
        WHERE {where}
        """,
        parameters={
            "minb": min_block,
            "maxb": max_block,
        },
    )

    assert len(result) == 1
    row = result.to_dicts()[0]
    return TimeRange(
        min=datetime_fromepoch(row["time_min"]),
        max=datetime_fromepoch(row["time_max"]),
        requested_max_timestamp=None,
    )


def chain_max_block(chain: str) -> ChainMaxBlock:
    """Find the max block ingested to goldsky for a given chain."""

    result = run_query_goldsky(
        query=f"""
        SELECT
            max(number) as block_max,
            max(timestamp) as timestamp_max
        FROM {chain}_blocks
        """
    )

    assert len(result) == 1
    row = result.to_dicts()[0]
    max_block = ChainMaxBlock(
        chain=chain,
        ts=row["timestamp_max"],
        number=row["block_max"],
    )
    return max_block
