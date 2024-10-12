"""Ozone Layer Utilities

The ozone layer is a way of structuring parquet files in cloud storage that helps us meet
these two goals:

  - Incremental ingestion of raw onchain data by block number.
  - Integration with BigQuery using a hive partitioned BigQuery external table.

Given a range of block numbers we want to have a deterministic way of locating where the
data for those blocks is stored in GCS.
"""

from datetime import date
from typing import Callable

import polars as pl

from dataclasses import dataclass, field
from op_datasets.processing.blockrange import BlockRange
from op_datasets.coretables.read import filter_to_date


# The number of blocks that are processed in a single ozone micro-batch. The goal is that
# fetching raw data from Clickhouse takes only a few seconds even for the larger chains
# like Base. But also we we don't want the micro-batch size to be too small so that many
# small parquet files are created.
BLOCK_MOD = 2000


@dataclass
class BlockBatch:
    """Represents the blocks included in a processing batch.

    BlockBatch is structurally the same as a BlockRange, but the BlockBatch
    is constructed to match the BLOCK_MOD ozone micro-batch size."""

    min: int  # inclusive
    max: int  # exclusive

    def __len__(self):
        return self.max - self.min


@dataclass
class BatchInput:
    """Represents the input data neeeded to process a batch."""

    chain: str  # chain name
    dt: str  # YYYY-MM-DD
    block_batch: BlockBatch
    dataframes: dict[str, pl.DataFrame]


@dataclass
class BatchOutputLocation:
    """Represents the location of an output produced when processing a batch.

    Note that processing a batch can result in one ore more outputs. The location of
    the output can be used to reference the output. This can be helpful to read it
    back later or to check if it has already been produced (to avoid reprocessing).
    """

    namespace: str
    name: str
    path: str


@dataclass
class BatchOutputs:
    chain: str  # chain name
    dt: str  # YYYY-MM-DD
    block_range: BlockRange

    outputs: list[BatchOutputLocation] = field(default_factory=list)

    def construct_path(self, dataset: str):
        return construct_parquet_path(
            dataset=dataset,
            chain=self.chain,
            block_range=self.block_range,
            dt=self.dt,
        )

    def save_output(self, namespace: str, name: str, path: str):
        self.outputs.append(BatchOutputLocation(namespace, name, path))

    def to_polars(self):
        data = []
        for output in self.outputs:
            data.append(
                {
                    "dt": date.fromisoformat(self.dt),
                    "chain": self.chain,
                    "block_range_min": self.block_range.min,
                    "block_range_max": self.block_range.max,
                    "output_file_namespace": output.namespace,
                    "output_file_name": output.name,
                    "output_file_path": output.path,
                }
            )

        return pl.DataFrame(
            data,
            schema={
                "dt": pl.Date,
                "chain": pl.String,
                "block_range_min": pl.UInt64,
                "block_range_max": pl.UInt64,
                "output_file_namespace": pl.String,
                "output_file_name": pl.String,
                "output_file_path": pl.String,
            },
        )


def block_floor(block_range: BlockRange):
    """The block floor is used as a coarse index to locate a block range in cloud storage."""
    block_floor = block_range.min - (block_range.min % BLOCK_MOD)
    return f"{block_floor:012d}"


def construct_dataset_path(dataset: str, chain: str):
    return f"{dataset}/chain={chain}"


def construct_date_path(dataset: str, chain: str, dt: str):
    return f"{dataset}/chain={chain}/dt={dt}"


def construct_parquet_filename(block_range: BlockRange):
    return f"{block_floor(block_range)}.parquet"


def construct_parquet_path(dataset: str, chain: str, dt: str, block_range: BlockRange):
    return f"{dataset}/chain={chain}/dt={dt}/{construct_parquet_filename(block_range)}"


def split_block_range(block_range: BlockRange) -> list[BlockBatch]:
    coarse = int(block_floor(block_range))

    batches: list[BlockBatch] = []
    while coarse < block_range.max:
        task: BlockBatch = BlockRange(coarse, coarse + BLOCK_MOD)
        batches.append(task)

        coarse += BLOCK_MOD

    return batches


def split_dates(
    block_batch: BlockRange,
    data_reader: Callable[[BlockRange], dict[str, pl.DataFrame]],
):
    input_dataframes: dict[str, pl.DataFrame] = data_reader(block_batch)

    for dt in input_dataframes["blocks"]["dt"].unique().sort().to_list():
        # We filter to a single date to make sure our processing never straddles date boundaries.
        dataframes = filter_to_date(input_dataframes, dt)

        yield dt, dataframes
