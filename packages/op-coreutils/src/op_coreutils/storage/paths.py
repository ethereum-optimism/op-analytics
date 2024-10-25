import os
import socket
from dataclasses import dataclass
from datetime import date
from typing import Any, Generator, NewType

import polars as pl
import pyarrow as pa

from op_coreutils.time import now, date_fromstr

# Root path for a partitioned dataframe output.
SinkOutputRootPath = NewType("SinkOutputRootPath", str)

# A single object path in a partitioned dataframe output (includes the base bath).
SinkOutputPath = NewType("SinkOutputPath", str)

# A single object path for a sink marker. Markers are light objects that are used to
# indicate a sink output has been written successfully.
SinkMarkerPath = NewType("SinkMarkerPath", str)


@dataclass
class KeyValue:
    key: str
    value: str


@dataclass
class PartitionedPath:
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

    def dt_value(self) -> date:
        """Return the "dt" value if this partition has a "dt" column."""
        for col in self.partition_cols:
            if col.key == "dt":
                if isinstance(col.value, str):
                    return date_fromstr(col.value)
                else:
                    raise ValueError(
                        f"a string value is expected on the 'dt' partition column: got dt={col.value}"
                    )
        raise ValueError(f"partition does not have a 'dt' column: {self}")

    def safe_dt_value(self) -> date:
        try:
            return self.dt_value()
        except ValueError:
            return date_fromstr("1970-01-01")

    def partition_values_map(self) -> dict[str, str]:
        return {col.key: str(col.value) for col in self.partition_cols}

    def partition_key_value_list(self) -> list[dict[str, str]]:
        """Return a list of key,value dicts that is is useful to represent the partition as a Map type in an Arror table."""
        return [{"key": key, "value": value} for key, value in self.partition_values_map().items()]

    def partition_array_list(self) -> list[list[str]]:
        """Return a list of key,value dicts that is is useful to represent the partition as a Map type in an Arror table."""
        return [[key, value] for key, value in self.partition_values_map().items()]


@dataclass
class PartitionedOutput:
    """Represent a single object written to storage."""

    path: PartitionedPath
    row_count: int


@dataclass
class Marker:
    """Represent a marker for a collection of objects written to storage."""

    marker_path: SinkMarkerPath
    dataset_name: str
    outputs: list[PartitionedOutput]
    chain: str
    process_name: str

    def to_rows(self) -> list[dict[str, Any]]:
        current_time = now()
        hostname = socket.gethostname()
        rows = []
        for parquet_out in self.outputs:
            rows.append(
                {
                    "updated_at": current_time,
                    "marker_path": self.marker_path,
                    "dataset_name": self.dataset_name,
                    "data_path": parquet_out.path.full_path,
                    "row_count": parquet_out.row_count,
                    "chain": self.chain,
                    "dt": parquet_out.path.safe_dt_value(),
                    "process_name": self.process_name,
                    "writer_name": hostname,
                }
            )
        return rows

    def arrow_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("updated_at", pa.timestamp(unit="us", tz=None)),
                pa.field("marker_path", pa.string()),
                pa.field("dataset_name", pa.string()),
                pa.field("data_path", pa.string()),
                pa.field("row_count", pa.int64()),
                pa.field("chain", pa.string()),
                pa.field("dt", pa.date32()),
                pa.field("process_name", pa.string()),
                pa.field("writer_name", pa.string()),
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
