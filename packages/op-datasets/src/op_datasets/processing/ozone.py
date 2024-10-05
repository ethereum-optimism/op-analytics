"""Ozone Layer Utilities

The ozone layer is a way of structuring parquet files in cloud storage that helps us meet
these two goals:

  - Incremental ingestion of raw onchain data by block number.
  - Integration with BigQuery using a hive partitioned BigQuery external table.

Given a range of block numbers we want to have a deterministic way of locating where the
data for those blocks is stored in GCS.
"""

from dataclasses import dataclass
from op_datasets.blockrange import BlockRange


# We use 10k blocks as a coarse index.
BLOCK_MOD = 10000


@dataclass
class UnitOfWork:
    chain: str  # chain name
    dt: str  # YYYY-MM-DD
    block_range: BlockRange

    def construct_path(self, dataset: str):
        basename = f"{block_floor(self.block_range)}__{self.block_range.min}-{self.block_range.max}.parquet"
        return f"{dataset}/chain={self.chain}/dt={self.dt}/{basename}"


def block_floor(block_range: BlockRange):
    """The block floor is used as a coarse index to locate a block range in cloud storage."""
    block_floor = block_range.min - (block_range.min % BLOCK_MOD)
    return f"{block_floor:012d}"


def determine_tasks(block_range: BlockRange) -> list[BlockRange]:
    coarse = int(block_floor(block_range))
    fine = block_range.min

    tasks = []
    while coarse < block_range.max:
        task = BlockRange(fine, min(coarse + BLOCK_MOD, block_range.max))
        tasks.append(task)

        coarse += BLOCK_MOD
        fine = task.max
    return tasks


@dataclass
class OverwriteTask:
    original_task: UnitOfWork
    new_task: UnitOfWork
    files_to_delete: list[str]


def resolve_overwrite(task: UnitOfWork) -> OverwriteTask:
    task.block_floor
