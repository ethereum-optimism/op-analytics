from dataclasses import dataclass
import polars as pl
from op_coreutils.logger import structlog

from op_datasets.pipeline.ozone import BlockBatch
from op_datasets.schemas import CoreDataset


log = structlog.get_logger()


class CoreDatasetSource:
    @classmethod
    def from_spec(cls, source_spec: str) -> "CoreDatasetSource":
        if source_spec.startswith("goldsky"):
            return GoldskySource()

        if source_spec.startswith("file://"):
            return LocalFileSource(basepath=source_spec.removeprefix("file://"))

        raise NotImplementedError()

    def read_from_source(
        self,
        datasets: dict[str, CoreDataset],
        block_batch: BlockBatch,
    ):
        raise NotImplementedError()


@dataclass
class GoldskySource(CoreDatasetSource):
    def read_from_source(
        self,
        datasets: dict[str, CoreDataset],
        block_batch: BlockBatch,
    ):
        from op_datasets.coretables import fromgoldsky

        return fromgoldsky.read_core_tables(datasets, block_batch)


@dataclass
class LocalFileSource(CoreDatasetSource):
    basepath: str

    def read_from_source(
        self,
        datasets: dict[str, CoreDataset],
        block_batch: BlockBatch,
    ):
        from op_datasets.coretables import fromlocal

        return fromlocal.read_core_tables(self.basepath, datasets, block_batch)


def filter_to_date(dataframes: dict[str, pl.DataFrame], dt: str):
    """Filter dataframes so they only contain data for blocks in the given "dt" partition."""
    blocks = dataframes["blocks"]

    filtered_blocks = blocks.filter(pl.col("dt") == dt)
    min_block = filtered_blocks.select("number").min().item()
    max_block = filtered_blocks.select("number").max().item()

    result = {}
    for name, df in dataframes.items():
        if name == "blocks":
            result[name] = filtered_blocks

        else:
            filtered = df.filter(
                (pl.col("block_number") >= min_block) & (pl.col("block_number") <= max_block)
            )
            filtered = filtered.with_columns(dt=pl.lit(dt)).select(["dt"] + filtered.columns)
            result[name] = filtered
    return result
