import itertools
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

import pyarrow as pa
import polars as pl
from op_coreutils.logger import structlog
from op_coreutils.partitioned import (
    DataLocation,
    OutputData,
    ExpectedOutput,
    SinkMarkerPath,
    SinkOutputRootPath,
    DataWriter,
)

from op_datasets.schemas import ONCHAIN_CURRENT_VERSION, CoreDataset

from .batches import BlockBatch
from .markers import INGESTION_MARKERS_TABLE
from .sources import RawOnchainDataProvider

log = structlog.get_logger()


@dataclass(kw_only=True)
class IngestionTask:
    """Contains all the information and data required to ingest a batch.

    This object is mutated during the ingestion process."""

    # If the task was constructed with a DateRange specification we store
    # the max timestamp of the range.
    max_requested_timestamp: int | None

    # Batch
    block_batch: BlockBatch

    # Source
    read_from: RawOnchainDataProvider

    # Inputs
    input_datasets: dict[str, CoreDataset]
    input_dataframes: dict[str, pl.DataFrame]
    inputs_ready: bool

    # Outputs
    output_dataframes: list[OutputData]

    # DataWriter
    data_writer: DataWriter

    # Progress Indicator
    progress_indicator: str

    @property
    def chain(self):
        return self.block_batch.chain

    @property
    def contextvars(self):
        ctx = self.block_batch.contextvars
        if self.progress_indicator:
            ctx["task"] = self.progress_indicator
        return ctx

    @classmethod
    def new(
        cls,
        max_requested_timestamp: int | None,
        block_batch: BlockBatch,
        read_from: RawOnchainDataProvider,
        write_to: list[DataLocation],
    ):
        expected_outputs = {}
        for name, dataset in ONCHAIN_CURRENT_VERSION.items():
            # Determine the marker path for this dataset.
            marker_path = block_batch.construct_marker_path()
            full_marker_path = SinkMarkerPath(f"markers/{dataset.versioned_location}/{marker_path}")

            # Construct expected output for the dataset.
            expected_outputs[name] = ExpectedOutput(
                dataset_name=name,
                root_path=SinkOutputRootPath(f"{dataset.versioned_location}"),
                file_name=block_batch.construct_parquet_filename(),
                marker_path=full_marker_path,
                process_name="default",
                additional_columns=dict(
                    num_blocks=block_batch.num_blocks(),
                    min_block=block_batch.min,
                    max_block=block_batch.max,
                ),
                additional_columns_schema=[
                    pa.field("chain", pa.string()),
                    pa.field("dt", pa.date32()),
                    pa.field("num_blocks", pa.int32()),
                    pa.field("min_block", pa.int64()),
                    pa.field("max_block", pa.int64()),
                ],
            )

        return cls(
            max_requested_timestamp=max_requested_timestamp,
            block_batch=block_batch,
            input_datasets={},
            input_dataframes={},
            inputs_ready=False,
            output_dataframes=[],
            read_from=read_from,
            data_writer=DataWriter(
                write_to=write_to,
                markers_table=INGESTION_MARKERS_TABLE,
                expected_outputs=expected_outputs,
                is_complete=False,
                force=False,
            ),
            progress_indicator="",
        )

    def add_inputs(self, datasets: dict[str, CoreDataset], dataframes: dict[str, pl.DataFrame]):
        for name, dataset in datasets.items():
            self.input_datasets[name] = dataset
            self.input_dataframes[name] = dataframes[name]

    def store_output(self, output: OutputData):
        self.output_dataframes.append(output)


def ordered_task_list(tasks: list[Any]):
    """Order tasks so that chains are visited in a round-robin fashion.

    This can be useful to ensure that progress is made on all chains in a fair manner.
    """
    chain_tasks = defaultdict(list)
    for task in tasks:
        chain_tasks[task.chain].append(task)

    # Get the num number of tasks for a chain.
    chain_min_tasks = min([len(_) for _ in chain_tasks.values()])

    chain_iters = {}
    for chain, task_list in chain_tasks.items():
        batch_size = len(task_list) // chain_min_tasks
        chain_iters[chain] = iter(itertools.batched(task_list, batch_size))

    pending = set(chain_tasks.keys())

    while pending:
        for chain in chain_iters:
            if chain not in pending:
                continue
            try:
                group = next(chain_iters[chain])
                for task in group:
                    yield task
            except StopIteration:
                pending.remove(chain)
