from typing import Any, Generator

import polars as pl


from .types import SinkOutputRootPath
from .output import WrittenParquetPath, KeyValue


def breakout_partitions(
    df: pl.DataFrame,
    partition_cols: list[str],
    root_path: SinkOutputRootPath,
    basename: str,
    default_partition: dict[str, Any] | None = None,
) -> Generator[tuple[pl.DataFrame, WrittenParquetPath], None, None]:
    """Split a dataframe into partitions.

    The data in each partition will be written to storage as a separate parquet file.
    """
    parts = df.select(*partition_cols).unique().to_dicts()

    if len(df) == 0:
        assert default_partition is not None
        yield (
            df.drop(*partition_cols),
            WrittenParquetPath(
                root=root_path,
                basename=basename,
                partitions=[KeyValue(key=col, value=val) for col, val in default_partition.items()],
                row_count=0,
            ),
        )

    else:
        for part in parts:
            part_df = df.filter(pl.all_horizontal(pl.col(col) == val for col, val in part.items()))

            yield (
                part_df.drop(*partition_cols),
                WrittenParquetPath(
                    root=root_path,
                    basename=basename,
                    partitions=[KeyValue(key=col, value=val) for col, val in part.items()],
                    row_count=len(part_df),
                ),
            )
