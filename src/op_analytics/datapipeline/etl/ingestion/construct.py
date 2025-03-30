import polars as pl
import pyarrow as pa

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dataaccess import check_marker
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.coreutils.partitioned.output import ExpectedOutput
from op_analytics.coreutils.partitioned.writerpartitioned import PartitionedWriteManager
from op_analytics.coreutils.rangeutils.blockrange import ChainMaxBlock

from .batches import BlockBatch, split_block_range
from .reader.request import BlockBatchRequest, BLOCKBATCH_MARKERS_TABLE
from .reader.rootpaths import RootPath
from .sources import RawOnchainDataProvider
from .task import IngestionTask

log = structlog.get_logger()


def construct_tasks(
    chains: list[str],
    range_spec: str,
    read_from: RawOnchainDataProvider,
    write_to: DataLocation,
):
    # Prepare the request
    blockbatch_request = BlockBatchRequest.build(
        chains=chains,
        range_spec=range_spec,
        root_paths_to_read=[
            RootPath.of("ingestion/blocks_v1"),
            RootPath.of("ingestion/transactions_v1"),
            RootPath.of("ingestion/logs_v1"),
            RootPath.of("ingestion/traces_v1"),
        ],
    )

    # Pre-fetch completion markers so we can skip completed tasks.
    markers_df = blockbatch_request.query_markers(location=write_to, padded_dates=True)

    # Batches to be ingested for each chain.
    chain_batches: dict[str, list[BlockBatch]] = ranges_to_batches(blockbatch_request)

    # Collect a single list of tasks to perform across all chains.
    all_tasks: list[IngestionTask] = []
    for batches in chain_batches.values():
        for batch in batches:
            all_tasks.append(
                new_task(
                    chain_max_block=blockbatch_request.chain_max_blocks[batch.chain],
                    requested_max_timestamp=blockbatch_request.time_range.requested_max_timestamp,
                    block_batch=batch,
                    read_from=read_from,
                    write_to=write_to,
                    output_markers_df=markers_df,
                )
            )

    return all_tasks


def ranges_to_batches(blockbatch_request: BlockBatchRequest):
    """For each chain split the block range into block batches to be processed."""
    chain_batches: dict[str, list[BlockBatch]] = {}

    for chain, chain_block_range in blockbatch_request.chain_block_ranges.items():
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

    return chain_batches


def new_task(
    chain_max_block: ChainMaxBlock,
    requested_max_timestamp: int | None,
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

        # Use the blocks dataset to check for overlapping markers.
        if name == "blocks" and output_markers_df is not None:
            overlapping = overlapping_markers(
                markers_df=output_markers_df,
                root_path=eo.root_path,
                block_batch=block_batch,
            )
            if overlapping:
                raise Exception(
                    f"overlapping markers found: {block_batch} overlaps with {overlapping}"
                )

        if check_marker(markers_df=output_markers_df, marker_path=eo.marker_path):
            complete_markers.append(eo.marker_path)

    return IngestionTask(
        chain_max_block=chain_max_block,
        requested_max_timestamp=requested_max_timestamp,
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
            markers_table=BLOCKBATCH_MARKERS_TABLE,
            expected_outputs=expected_outputs,
            complete_markers=complete_markers,
        ),
        progress_indicator="",
    )


def overlapping_markers(
    markers_df: pl.DataFrame,
    root_path: str,
    block_batch: BlockBatch,
) -> str | None:
    """Check if there are any overlapping markers for a given block range.

    If there are overlapping markers, this means we have made mistake in setting the
    block batches (see batches.py).
    """
    overlapping = markers_df.filter(
        pl.col("root_path") == root_path,
        pl.col("chain") == block_batch.chain,
        pl.col("min_block") <= block_batch.max,
        pl.col("max_block") >= block_batch.min,
    )
    if overlapping.is_empty():
        return None

    return overlapping["marker_path"].to_list()
