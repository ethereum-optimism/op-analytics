import polars as pl
from op_coreutils.logger import structlog

from op_datasets.blockrange import BlockRange
from op_datasets.datastores import DataSource, GoldskySource, LocalFileSource

log = structlog.get_logger()


def read_core_tables(
    chain: str,
    datasource: DataSource,
    block_range: BlockRange,
) -> dict[str, pl.DataFrame]:
    if isinstance(datasource, GoldskySource):
        from op_datasets.coretables import fromgoldsky

        return fromgoldsky.read_core_tables(chain, block_range)

    if isinstance(datasource, LocalFileSource):
        from op_datasets.coretables import fromlocal

        return fromlocal.read_core_tables(chain, block_range)

    raise NotImplementedError()


def filter_to_date(dataframes: dict[str, pl.DataFrame], dt: str):
    """Filter dataframes so they only contain data for blocks in the given "dt" partition."""
    blocks = dataframes["blocks"]

    filtered_blocks = blocks.filter(pl.col("dt") == dt)
    min_block = filtered_blocks.select("number").min().item()
    max_block = filtered_blocks.select("number").max().item()
    log.info("Filtering blocks", dt=dt, min_block=min_block, max_block=max_block)

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
