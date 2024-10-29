import itertools
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

import polars as pl
from op_coreutils.logger import structlog
from op_coreutils.partitioned import (
    DataLocation,
    OutputDataFrame,
    SinkMarkerPath,
    SinkOutputRootPath,
)

from op_datasets.schemas import ONCHAIN_CURRENT_VERSION, CoreDataset

from .batches import BlockBatch
from .sources import RawOnchainDataProvider

log = structlog.get_logger()


@dataclass(kw_only=True)
class IngestionTask:
    """Contains all the information and data required to ingest a batch.

    This object is mutated during the ingestion process."""

    # Batch
    block_batch: BlockBatch

    # Source
    read_from: RawOnchainDataProvider

    # Inputs
    input_datasets: dict[str, CoreDataset]
    input_dataframes: dict[str, pl.DataFrame]
    inputs_ready: bool

    # Outputs
    output_dataframes: list[OutputDataFrame]
    force: bool  # ignores completion markers when set to true

    # Sinks
    write_to: list[DataLocation]

    # Expected Markers
    expected_markers: list[SinkMarkerPath]
    is_complete: bool

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
        block_batch: BlockBatch,
        read_from: RawOnchainDataProvider,
        write_to: list[DataLocation],
    ):
        new_obj = cls(
            block_batch=block_batch,
            input_datasets={},
            input_dataframes={},
            inputs_ready=False,
            expected_markers=[],
            is_complete=False,
            output_dataframes=[],
            force=False,
            read_from=read_from,
            write_to=write_to,
            progress_indicator="",
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
