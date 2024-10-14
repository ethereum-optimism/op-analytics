import polars as pl
from op_coreutils.logger import structlog

from op_datasets.processing.blockrange import BlockRange
from op_datasets.schemas import CoreDataset
from typing import Literal, Union
from pydantic import BaseModel, Field

log = structlog.get_logger()


class GoldskySource(BaseModel):
    source_type: Literal["goldsky"] = "goldsky"


class LocalFileSource(BaseModel):
    basepath: str

    source_type: Literal["localfile"] = "localfile"


class DataSource(BaseModel):
    source: Union[GoldskySource, LocalFileSource] = Field(..., discriminator="source_type")

    @classmethod
    def from_spec(cls, source_spec: str) -> "DataSource":
        if source_spec.startswith("goldsky"):
            return GoldskySource()

        if source_spec.startswith("file://"):
            return LocalFileSource(basepath=source_spec.removeprefix("file://"))

        raise NotImplementedError()


def read_core_datasets(
    chain: str,
    source_spec: str,
    datasets: dict[str, CoreDataset],
    block_range: BlockRange,
) -> dict[str, pl.DataFrame]:
    datasource = DataSource.from_spec(source_spec)

    # Read
    dataframes = read_from_source(
        datasource=datasource,
        chain=chain,
        datasets=datasets,
        block_range=block_range,
    )

    # Run the enrichment process
    if isinstance(datasource, GoldskySource):
        return enrichment(datasets=datasets, dataframes=dataframes)

    return dataframes


def read_from_source(
    datasource: DataSource,
    chain: str,
    datasets: dict[str, CoreDataset],
    block_range: BlockRange,
):
    if isinstance(datasource, GoldskySource):
        from op_datasets.coretables import fromgoldsky

        return fromgoldsky.read_core_tables(chain, datasets, block_range)

    if isinstance(datasource, LocalFileSource):
        from op_datasets.coretables import fromlocal

        return fromlocal.read_core_tables(datasource.basepath, chain, datasets, block_range)

    raise NotImplementedError()


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


def enrichment(
    datasets: dict[str, CoreDataset],
    dataframes: dict[str, pl.DataFrame],
) -> dict[str, pl.DataFrame]:
    new_dataframes = {}
    for dataset_name, dataset in datasets.items():
        enriched_columns = {}
        for column_name, enrichment_func in dataset.enrichment_functions().items():
            enriched_columns[column_name] = enrichment_func(dataframes)

        if enriched_columns:
            new_dataframes[dataset_name] = dataframes[dataset_name].with_columns(**enriched_columns)
        else:
            new_dataframes[dataset_name] = dataframes[dataset_name]

    return new_dataframes
