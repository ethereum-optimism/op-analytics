"""Ozone Layer Utilities

The ozone layer is a way of structuring parquet files in cloud storage that helps us meet
these two goals:

  - Incremental ingestion of raw onchain data by block number.
  - Integration with BigQuery using a hive partitioned BigQuery external table.

Given a range of block numbers we want to have a deterministic way of locating where the
data for those blocks is stored in GCS.
"""

from datetime import date

import polars as pl

from dataclasses import dataclass, field
from op_datasets.processing.blockrange import BlockRange


@dataclass
class BlockBatch:
    """Represents a single processing batch.

    BlockBatch is structurally the same as a BlockRange, but the BlockBatch is
    constructed to match the block bach boundaries configured for each chain."""

    chain: str
    min: int  # inclusive
    max: int  # exclusive

    def __len__(self):
        return self.max - self.min

    def filter(self, number_column: str = "number"):
        return f" {number_column} >= {self.min} and {number_column} < {self.max}"

    def construct_parquet_filename(self):
        return f"{self.min:012d}.parquet"

    def construct_dataset_path(self, dataset):
        return f"{dataset}/chain={self}"

    def construct_date_path(self, dataset: str, dt: str):
        return f"{dataset}/chain={self.chain}/dt={dt}"

    def construct_parquet_path(self, dataset: str, dt: str):
        return f"{dataset}/chain={self.chain}/dt={dt}/{self.construct_parquet_filename()}"


# The number of blocks that are processed in a single ozone micro-batch. The goal is that
# fetching raw data from Clickhouse takes only a few seconds even for the larger chains
# like Base. But also we we don't want the micro-batch size to be too small so that many
# small parquet files are created.
BLOCK_MOD = 2000

# For each chain we configure the block batch size that should be used. The batch size
# can change over time for a change, so the configuration stores the block numbers at
# which the batch size changes. The idea here is that at the start of the chain or for
# some smaller chains we can use very large batches. For chains that grow in usage we
# have to use progressively smaller batches.


@dataclass
class Delimiter:
    block_number: int
    batch_size: int

    @property
    def next_block(self):
        return self.block_number + self.batch_size


MICROBATCH_SIZE_CONFIGURATION = {
    # Non-default
    "base": [
        Delimiter(block_number=0, batch_size=10000),
        Delimiter(block_number=10000000, batch_size=5000),
        Delimiter(block_number=15000000, batch_size=2000),
    ],
    "op": [
        Delimiter(block_number=0, batch_size=10000),
        Delimiter(block_number=62000000, batch_size=5000),
        Delimiter(block_number=94000000, batch_size=2000),
    ],
    # Default
    "bob": [Delimiter(0, 20000)],
    "cyber": [Delimiter(0, 20000)],
    "fraxtal": [Delimiter(0, 20000)],
    "ham": [Delimiter(0, 20000)],
    "kroma": [Delimiter(0, 20000)],
    "lisk": [Delimiter(0, 20000)],
    "lyra": [Delimiter(0, 20000)],
    "metal": [Delimiter(0, 20000)],
    "mint": [Delimiter(0, 20000)],
    "mode": [Delimiter(0, 20000)],
    "orderly": [Delimiter(0, 20000)],
    "polynomial": [Delimiter(0, 20000)],
    "race": [Delimiter(0, 20000)],
    "redstone": [Delimiter(0, 20000)],
    "swan": [Delimiter(0, 20000)],
    "xterio": [Delimiter(0, 20000)],
    "zora": [Delimiter(0, 20000)],
}


class InvalidMicrobatchConfig(Exception):
    pass


def validate_microbatch_configuration(boundaries: list[Delimiter]):
    for i, boundary in enumerate(boundaries):
        if i == 0:
            if boundary.block_number != 0:
                raise InvalidMicrobatchConfig(
                    f"the first delimiter should always start at block 0: {boundary}"
                )

        else:
            prev_boundary = boundaries[i - 1]
            distance = boundary.block_number - prev_boundary.block_number
            if distance % prev_boundary.batch_size != 0:
                raise InvalidMicrobatchConfig(
                    f"delimiter block number should align: {prev_boundary} -> {boundary}"
                )


for config in MICROBATCH_SIZE_CONFIGURATION.values():
    validate_microbatch_configuration(config)


def find_batch_delimiter(boundaries: list[Delimiter], block_number: int) -> Delimiter:
    padded_start_boundaries = [boundaries[0]] + boundaries
    padded_end_boundaries = boundaries + [boundaries[-1]]

    for prev_delimiter, delimiter in zip(padded_start_boundaries, padded_end_boundaries):
        assert delimiter.block_number % delimiter.batch_size == 0

        if delimiter.block_number <= block_number:
            continue
        else:
            break

    return Delimiter(
        block_number=block_number - (block_number % prev_delimiter.batch_size),
        batch_size=prev_delimiter.batch_size,
    )


def split_block_range(chain: str, block_range: BlockRange) -> list[BlockBatch]:
    boundaries = MICROBATCH_SIZE_CONFIGURATION[chain]
    return split_block_range_from_boundaries(chain, boundaries, block_range)


def split_block_range_from_boundaries(
    chain: str,
    boundaries: list[(int, int)],
    block_range: BlockRange,
) -> list[BlockBatch]:
    validate_microbatch_configuration(boundaries)

    delimiter = find_batch_delimiter(boundaries, block_range.min)

    batches: list[BlockBatch] = []
    while delimiter.block_number < block_range.max:
        new_delimiter = find_batch_delimiter(boundaries, delimiter.next_block)
        if delimiter.next_block != new_delimiter.block_number:
            raise ValueError(f"inconsisent batch: {delimiter}  ..  {new_delimiter}")
        batches.append(BlockBatch(chain, delimiter.block_number, delimiter.next_block))
        delimiter = new_delimiter
    return batches


@dataclass
class BatchInput:
    """Represents the input data neeeded to process a batch."""

    dt: str  # YYYY-MM-DD
    block_batch: BlockBatch
    dataframes: dict[str, pl.DataFrame]

    @property
    def chain(self):
        return self.block_batch.chain


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
    dt: str  # YYYY-MM-DD
    block_batch: BlockBatch

    outputs: list[BatchOutputLocation] = field(default_factory=list)

    def construct_path(self, dataset: str):
        return self.block_batch.construct_parquet_path(dataset=dataset, dt=self.dt)

    def save_output(self, namespace: str, name: str, path: str):
        self.outputs.append(BatchOutputLocation(namespace, name, path))

    def to_polars(self):
        data = []
        for output in self.outputs:
            data.append(
                {
                    "dt": date.fromisoformat(self.dt),
                    "chain": self.block_batch.chain,
                    "block_range_min": self.block_batch.min,
                    "block_range_max": self.block_batch.max,
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
