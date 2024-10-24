from dataclasses import dataclass

import polars as pl
from op_coreutils.logger import structlog
from op_coreutils.storage.paths import SinkMarkerPath, SinkOutputRootPath

from op_datasets.schemas import ONCHAIN_CURRENT_VERSION, CoreDataset

from .batches import BlockBatch
from .utilities import RawOnchainDataProvider, RawOnchainDataLocation


log = structlog.get_logger()


@dataclass
class OutputDataFrame:
    dataframe: pl.DataFrame
    root_path: SinkOutputRootPath
    marker_path: SinkMarkerPath


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
    write_to: list[RawOnchainDataLocation]

    # Expected Markers
    expected_markers: list[SinkMarkerPath]
    is_complete: bool

    @property
    def chain(self):
        return self.block_batch.chain

    @property
    def contextvars(self):
        return dict(
            chain=self.block_batch.chain,
            blocks=f"#{self.block_batch.min}-{self.block_batch.max}",
        )

    @classmethod
    def new(
        cls,
        block_batch: BlockBatch,
        read_from: RawOnchainDataProvider,
        write_to: list[RawOnchainDataLocation],
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
