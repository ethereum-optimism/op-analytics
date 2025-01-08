import polars as pl
import pyarrow as pa

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dataaccess import check_marker
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.coreutils.partitioned.output import ExpectedOutput
from op_analytics.coreutils.partitioned.writer import PartitionedWriteManager
from op_analytics.coreutils.rangeutils.blockrange import ChainMaxBlock

from .batches import BlockBatch, split_block_range
from .reader.markers import INGESTION_MARKERS_TABLE, IngestionDataSpec
from .reader.request import BlockBatchRequest
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
    blockbatch_request = BlockBatchRequest.build(chains, range_spec)

    # Pre-fetch completion markers so we can skip completed tasks.
    data_spec = IngestionDataSpec(chains=chains)
    markers_df = data_spec.query_markers(
        datevals=blockbatch_request.datevals,
        location=write_to,
    )

    # Batches to be ingested for each chain.
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

    # Collect a single list of tasks to perform across all chains.
    all_tasks: list[IngestionTask] = []
    for batches in chain_batches.values():
        for batch in batches:
            all_tasks.append(
                new_task(
                    chain_max_block=blockbatch_request.chain_max_blocks[chain],
                    requested_max_timestamp=blockbatch_request.requested_max_timestamp,
                    block_batch=batch,
                    read_from=read_from,
                    write_to=write_to,
                    output_markers_df=markers_df,
                )
            )

    return all_tasks


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
            markers_table=INGESTION_MARKERS_TABLE,
            expected_outputs=expected_outputs,
            complete_markers=complete_markers,
        ),
        progress_indicator="",
    )
