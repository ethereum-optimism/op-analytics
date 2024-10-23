from dataclasses import dataclass

import polars as pl
from op_coreutils.logger import structlog
from op_coreutils.storage.paths import SinkMarkerPath, SinkOutputRootPath
from op_coreutils.threads import run_concurrently

from op_datasets.utils.blockrange import BlockRange
from op_datasets.utils.daterange import DateRange
from op_datasets.schemas import ONCHAIN_CURRENT_VERSION, CoreDataset

from .batches import BlockBatch, split_block_range
from .sinks import DataSink
from .sources import CoreDatasetSource
from .utilities import block_range_for_dates

log = structlog.get_logger()


@dataclass
class OutputDataFrame:
    dataframe: pl.DataFrame
    root_path: SinkOutputRootPath
    marker_path: SinkMarkerPath


@dataclass
class IngestionTask:
    """Contains all the information and data required to ingest a batch.

    This object is mutated during the ingestion process."""

    # Batch
    block_batch: BlockBatch

    # Source
    source: CoreDatasetSource

    # Inputs
    input_datasets: dict[str, CoreDataset]
    input_dataframes: dict[str, pl.DataFrame]

    # Outputs
    output_dataframes: list[OutputDataFrame]
    force: bool  # ignores completion markers when set to true

    # Sinks
    sinks: list[DataSink]

    # Expected Markers
    expected_markers: list[SinkMarkerPath]
    is_complete: bool

    @property
    def chain(self):
        return self.block_batch.chain

    @property
    def pretty(self):
        return f"{self.chain}#{self.block_batch.min}"

    @classmethod
    def new(
        cls,
        block_batch: BlockBatch,
        source: CoreDatasetSource,
        sinks: list[DataSink],
    ):
        new_obj = cls(
            block_batch=block_batch,
            input_datasets={},
            input_dataframes={},
            expected_markers=[],
            is_complete=False,
            output_dataframes=[],
            force=False,
            source=source,
            sinks=sinks,
        )

        for dataset in ONCHAIN_CURRENT_VERSION.values():
            new_obj.expected_markers.append(new_obj.get_marker_location(dataset))

        return new_obj

    def add_inputs(self, datasets: dict[str, CoreDataset], dataframes: dict[str, pl.DataFrame]):
        for key, val in datasets.items():
            self.add_input(key, val, dataframes[key])

    def add_input(self, name: str, dataset: CoreDataset, dataframe: pl.DataFrame):
        self.input_datasets[name] = dataset
        self.input_dataframes[name] = dataframe

    def add_output(self, output: OutputDataFrame):
        self.output_dataframes.append(output)

    def get_output_location(self, dataset: CoreDataset) -> SinkOutputRootPath:
        return SinkOutputRootPath(f"{dataset.versioned_location}")

    def get_marker_location(self, dataset: CoreDataset) -> SinkMarkerPath:
        marker_path = self.block_batch.construct_marker_path()
        return SinkMarkerPath(f"markers/{dataset.versioned_location}/{marker_path}")


def construct_sinks(
    sinks_spec: list[str],
) -> list[DataSink]:
    sinks = []
    for sink_spec in sinks_spec:
        if sink_spec == "local":
            # Use the canonical location in our repo
            sinks.append(DataSink(sink_spec="file://ozone/warehouse/"))
        elif sink_spec == "gcs":
            sinks.append(DataSink(sink_spec=sink_spec))
        else:
            raise NotImplementedError(f"sink_spec not supported: {sink_spec}")
    return sinks


def construct_tasks(
    chains: list[str],
    range_spec: str,
    source_spec: str,
    sinks_spec: list[str],
):
    blocks_by_chain: dict[str, BlockRange]

    try:
        block_range = BlockRange.from_spec(range_spec)
        blocks_by_chain = {}
        for chain in chains:
            blocks_by_chain[chain] = block_range

    except NotImplementedError:
        # Ensure range_spec is a valid DateRange.
        DateRange.from_spec(range_spec)

        def blocks_for_chain(ch):
            return block_range_for_dates(chain=ch, date_spec=range_spec)

        blocks_by_chain = run_concurrently(blocks_for_chain, targets=chains, max_workers=4)

    # Batches to be ingested for each chain.
    chain_batches: dict[str, list[BlockBatch]] = {}
    for chain, chain_block_range in blocks_by_chain.items():
        chain_batches[chain] = split_block_range(chain, chain_block_range)

    # Log a summary of the work that will be done for each chain.
    for chain, batches in chain_batches.items():
        total_blocks = batches[-1].max - batches[0].min
        log.info(
            f"Will process chain={chain!r} {len(batches)} batch(es) {total_blocks} total blocks starting at #{batches[0].min}"
        )

    # Source
    datasource = CoreDatasetSource.from_spec(source_spec)

    # Sinks
    sinks = construct_sinks(sinks_spec)

    # Collect a single list of tasks to perform across all chains.
    all_tasks: list[IngestionTask] = []
    for batches in chain_batches.values():
        for batch in batches:
            all_tasks.append(
                IngestionTask.new(
                    block_batch=batch,
                    source=datasource,
                    sinks=sinks,
                )
            )

    return all_tasks
