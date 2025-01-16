from typing import Generator

import polars as pl


from .partition import PartitionData


def breakout_partitions(
    df: pl.DataFrame,
    partition_cols: list[str],
    default_partitions: list[dict[str, str]] | None = None,
) -> Generator[PartitionData, None, None]:
    """Split a dataframe into partitions.

    The data in each partition will be written to storage as a separate parquet file.
    """
    parts = df.select(*partition_cols).unique().sort(*partition_cols).to_dicts()

    if len(df) == 0:
        assert default_partitions is not None
        for default_partition in default_partitions:
            yield PartitionData.from_dict(
                partitions_dict=default_partition,
                df=df.drop(*partition_cols),
            )

    else:
        for part in parts:
            part_df = df.filter(pl.all_horizontal(pl.col(col) == val for col, val in part.items()))

            yield PartitionData.from_dict(
                partitions_dict=part,
                df=part_df.drop(*partition_cols),
            )
