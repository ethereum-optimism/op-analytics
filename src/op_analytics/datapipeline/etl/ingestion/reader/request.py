from dataclasses import dataclass
from datetime import date

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.rangeutils.blockrange import BlockRange, ChainMaxBlock
from op_analytics.coreutils.rangeutils.timerange import TimeRange

from .ranges import get_chain_block_ranges, get_chain_max_blocks, time_range_for_blocks

log = structlog.get_logger()


@dataclass
class BlockBatchRequest:
    # Chains requested
    chains: list[str]

    # The block range requested for each chain.
    chain_block_ranges: dict[str, BlockRange]

    # The max block for each chain.
    chain_max_blocks: dict[str, ChainMaxBlock]

    # If the request was created with a DateRange specification we store
    # the max timestamp of the range.
    requested_max_timestamp: int | None

    # The date values associated with this request
    datevals: list[date]

    @classmethod
    def build(
        cls,
        chains: list[str],
        range_spec: str,
    ) -> "BlockBatchRequest":
        try:
            block_range = BlockRange.from_spec(range_spec)

            if len(chains) != 1:
                raise Exception(
                    "Ingesting by block_range is only supported for one chain at a time."
                )

            chain_block_ranges = {}
            for chain in chains:
                chain_block_ranges[chain] = block_range
            requested_max_timestamp = None

            # Determine the time range for the provided block range.
            time_range = time_range_for_blocks(
                chain=chains[0],
                min_block=block_range.min,
                max_block=block_range.max,
            )
            output_marker_datevals = time_range.to_date_range().dates()

        except NotImplementedError:
            time_range = TimeRange.from_spec(range_spec)

            # We need datevals to query completion markers so we can determine
            # which data is not ingested yet. Datevals are padded since block
            # batches may straddle a date boundary on either end.
            output_marker_datevals = time_range.to_date_range().padded_dates()
            block_range = None

            chain_block_ranges = get_chain_block_ranges(chains, time_range)
            requested_max_timestamp = time_range.requested_max_timestamp

        return cls(
            chains=chains,
            chain_block_ranges=chain_block_ranges,
            chain_max_blocks=get_chain_max_blocks(chains),
            requested_max_timestamp=requested_max_timestamp,
            datevals=output_marker_datevals,
        )
