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

    # Keep track of yielded partitions.
    # This is so we can guarantee full coverage of default_partitions list.
    yielded_parts = []

    if len(df) == 0:
        assert default_partitions is not None
        for default_partition in default_partitions:
            yielded_parts.append(default_partition)

            yield PartitionData.from_dict(
                partitions_dict=default_partition,
                df=df.drop(*partition_cols),
            )

    else:
        for part in parts:
            part_df = df.filter(pl.all_horizontal(pl.col(col) == val for col, val in part.items()))
            yielded_parts.append(part)

            yield PartitionData.from_dict(
                partitions_dict=part,
                df=part_df.drop(*partition_cols),
            )

    # The default partitions that are not observed on the actual data still need to
    # be yielded so that markers for them can be written out.
    for default_partition in default_partitions or []:
        if default_partition not in yielded_parts:
            yield PartitionData.from_dict(
                partitions_dict=default_partition,
                df=df.filter(False).drop(*partition_cols),
            )
