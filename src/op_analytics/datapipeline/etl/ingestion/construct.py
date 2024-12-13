import polars as pl
import pyarrow as pa

from op_analytics.coreutils.clickhouse.goldsky import run_query_goldsky
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dataaccess import check_marker
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.coreutils.partitioned.output import ExpectedOutput
from op_analytics.coreutils.partitioned.writer import PartitionedWriteManager
from op_analytics.coreutils.rangeutils.blockrange import BlockRange
from op_analytics.coreutils.rangeutils.timerange import TimeRange
from op_analytics.coreutils.threads import run_concurrently
from op_analytics.coreutils.time import datetime_fromepoch

from .reader.markers import INGESTION_MARKERS_TABLE, IngestionDataSpec
from .batches import BlockBatch, split_block_range
from .sources import RawOnchainDataProvider
from .task import IngestionTask

log = structlog.get_logger()


def construct_tasks(
    chains: list[str],
    range_spec: str,
    read_from: RawOnchainDataProvider,
    write_to: DataLocation,
):
    blocks_by_chain: dict[str, BlockRange]

    try:
        block_range = BlockRange.from_spec(range_spec)

        if len(chains) != 1:
            raise Exception("Ingesting by block_range is only supported for one chain at a time.")

        blocks_by_chain = {}
        for chain in chains:
            blocks_by_chain[chain] = block_range
        max_requested_timestamp = None

        # Determine the time range.
        time_range = time_range_for_blocks(
            chain=chains[0],
            min_block=block_range.min,
            max_block=block_range.max,
        )
        output_marker_datevals = time_range.to_date_range().dates()

    except NotImplementedError:
        time_range = TimeRange.from_spec(range_spec)

        # We need datevals to query completion markers so we can determine
        # which data is not ingested yet. Datevals are padded since the block
        # batches may straddle a date boundary on either end.
        output_marker_datevals = time_range.to_date_range().padded_dates()
        block_range = None

        def blocks_for_chain(ch):
            return block_range_for_dates(
                chain=ch,
                min_ts=time_range.min_ts,
                max_ts=time_range.max_ts,
            )

        blocks_by_chain = run_concurrently(blocks_for_chain, targets=chains, max_workers=4)
        max_requested_timestamp = time_range.max_requested_timestamp

    # Batches to be ingested for each chain.
    chain_batches: dict[str, list[BlockBatch]] = {}
    for chain, chain_block_range in blocks_by_chain.items():
        chain_batches[chain] = split_block_range(chain, chain_block_range)

    # Log a summary of the work that will be done for each chain.
    for chain, batches in chain_batches.items():
        if batches:
            total_blocks = batches[-1].max - batches[0].min
            log.info(
                f"prepared chain={chain}: {len(batches)} batch(es) {total_blocks} total blocks starting at #{batches[0].min}"
            )
        else:
            log.info(f"prepared chain={chain!r}: {len(batches)} batch(es)")

    # Pre-fetch completion markers so we can skip completed tasks.
    data_spec = IngestionDataSpec(chains=chains)
    markers_df = data_spec.query_markers(
        datevals=output_marker_datevals,
        read_from=write_to,
    )

    # Collect a single list of tasks to perform across all chains.
    all_tasks: list[IngestionTask] = []
    for batches in chain_batches.values():
        for batch in batches:
            all_tasks.append(
                new_task(
                    max_requested_timestamp=max_requested_timestamp,
                    block_batch=batch,
                    read_from=read_from,
                    write_to=write_to,
                    output_markers_df=markers_df,
                )
            )

    return all_tasks


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
    return BlockRange(row["block_min"], row["block_max"])


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
        max_requested_timestamp=row["time_max"],
    )


def new_task(
    max_requested_timestamp: int | None,
    block_batch: BlockBatch,
    read_from: RawOnchainDataProvider,
    write_to: DataLocation,
    output_markers_df: pl.DataFrame | None = None,
) -> IngestionTask:
    """Create a new IngestionTask instance."""

    expected_outputs: list[ExpectedOutput] = []
    complete_markers: list[str] = []

    for name in ["blocks", "transactions", "logs", "traces"]:
        # Determine the directory where we will write this dataset.
        data_directory = block_batch.dataset_directory(dataset_name=name)

        # Construct the ExpectedOutput.
        eo: ExpectedOutput = block_batch.construct_expected_output(root_path=data_directory)
        expected_outputs.append(eo)

        if check_marker(markers_df=output_markers_df, marker_path=eo.marker_path):
            complete_markers.append(eo.marker_path)

    return IngestionTask(
        max_requested_timestamp=max_requested_timestamp,
        block_batch=block_batch,
        input_dataframes={},
        output_dataframes=[],
        read_from=read_from,
        write_manager=PartitionedWriteManager(
            location=write_to,
            partition_cols=["chain", "dt"],
            extra_marker_columns=dict(
                num_blocks=block_batch.num_blocks(),
                min_block=block_batch.min,
                max_block=block_batch.max,
            ),
            extra_marker_columns_schema=[
                pa.field("chain", pa.string()),
                pa.field("dt", pa.date32()),
                pa.field("num_blocks", pa.int32()),
                pa.field("min_block", pa.int64()),
                pa.field("max_block", pa.int64()),
            ],
            markers_table=INGESTION_MARKERS_TABLE,
            expected_outputs=expected_outputs,
            complete_markers=complete_markers,
        ),
        progress_indicator="",
    )
