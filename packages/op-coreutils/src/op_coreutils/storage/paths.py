import os
from typing import Any, Generator, NewType

import polars as pl
import pyarrow as pa
from pydantic import BaseModel

# Root path for a partitioned dataframe output.
SinkOutputRootPath = NewType("SinkOutputRootPath", str)

# A single object path in a partitioned dataframe output (includes the base bath).
SinkOutputPath = NewType("SinkOutputPath", str)

# A single object path for a sink marker. Markers are light objects that are used to
# indicate a sink output has been written successfully.
SinkMarkerPath = NewType("SinkMarkerPath", str)


class KeyValue(BaseModel):
    key: str
    value: str


class PartitionedPath(BaseModel):
    root: SinkOutputRootPath
    basename: str
    partition_cols: list[KeyValue]

    @classmethod
    def from_partition(
        cls, root: SinkOutputRootPath, basename: str, partition: dict[str, str]
    ) -> "PartitionedPath":
        return cls(
            root=root,
            basename=basename,
            partition_cols=[KeyValue(key=col, value=val) for col, val in partition.items()],
        )

    @property
    def partitions_path(self):
        return "/".join(f"{col.key}={col.value}" for col in self.partition_cols)

    @property
    def full_path(self):
        return os.path.join(self.root, self.partitions_path, self.basename)

    def dt_value(self) -> str:
        """Return the "dt" value if this partition has a "dt" column."""
        for col in self.partition_cols:
            if col.key == "dt":
                if isinstance(col.value, str):
                    return col.value
                else:
                    raise ValueError(
                        f"a string value is expected on the 'dt' partition column: got dt={col.value}"
                    )
        raise ValueError(f"partition does not have a 'dt' column: {self}")

    def partition_values_map(self) -> dict[str, str]:
        return {col.key: str(col.value) for col in self.partition_cols}

    def partition_key_value_list(self) -> list[dict[str, str]]:
        """Return a list of key,value dicts that is is useful to represent the partition as a Map type in an Arror table."""
        return [{"key": key, "value": value} for key, value in self.partition_values_map().items()]


class PartitionedOutput(BaseModel):
    """Represent a single object written to storage."""

    path: PartitionedPath
    row_count: int


class Marker(BaseModel):
    """Represent a marker for a collection of objects written to storage."""

    marker_path: SinkMarkerPath
    outputs: list[PartitionedOutput]

    @property
    def total_rows(self):
        return sum(_.row_count for _ in self.outputs)

    def to_row(self):
        return self.model_dump()

    def to_clickhouse_row(self) -> dict[str, Any]:
        full_paths: list[str] = []
        partition_cols: list[list[dict[str, str]]] = []
        row_counts: list[int] = []
        total_rows: int = 0

        for output in self.outputs:
            full_paths.append(output.path.full_path)
            partition_cols.append(output.path.partition_key_value_list())
            row_counts.append(output.row_count)
            total_rows += output.row_count

        return {
            "marker_path": self.marker_path,
            "total_rows": total_rows,
            "outputs.full_path": full_paths,
            "outputs.partition_cols": partition_cols,
            "outputs.row_count": row_counts,
        }

    def clickhouse_schema(self):
        return pa.schema(
            [
                pa.field("marker_path", pa.string()),
                pa.field("total_rows", pa.int64()),
                pa.field("outputs.full_path", pa.large_list(pa.string())),
                pa.field(
                    "outputs.partition_cols", pa.large_list(pa.map_(pa.string(), pa.string()))
                ),
                pa.field("outputs.row_count", pa.large_list(pa.int64())),
            ]
        )


def breakout_partitions(
    df: pl.DataFrame,
    partition_cols: list[str],
    root_path: SinkOutputRootPath,
    basename: str,
) -> Generator[tuple[pl.DataFrame, PartitionedOutput], None, None]:
    parts = df.select(*partition_cols).unique().to_dicts()

    for part in parts:
        part_df = df.filter(pl.all_horizontal(pl.col(col) == val for col, val in part.items()))

        yield (
            part_df.drop(*partition_cols),
            PartitionedOutput(
                path=PartitionedPath.from_partition(
                    root=root_path,
                    basename=basename,
                    partition=part,
                ),
                row_count=len(part_df),
            ),
        )
