import os
from dataclasses import dataclass
from typing import Any

import polars as pl
import pyarrow as pa

from .types import SinkMarkerPath, SinkOutputRootPath


@dataclass
class ExpectedOutput:
    """Information about a dataset that is expectd to be produced by a task."""

    # Name of the datset.
    dataset_name: str

    # Completion marker path.
    marker_path: SinkMarkerPath

    # Identifier for the process that produced the datset.
    process_name: str

    # Values for additional columns stored in the markers table.
    additional_columns: dict[str, Any]

    # Schema for additional columns stored in the markers table.
    # This schema is used to create a pyarrow table to write markers
    # into the markers table.
    additional_columns_schema: list[pa.Field]


@dataclass
class OutputData:
    dataframe: pl.DataFrame
    root_path: SinkOutputRootPath
    dataset_name: str

    # Default partition values for cases when the output datafarame is empty
    # and therefore has no implicit partition values.
    default_partition: dict[str, Any]


@dataclass
class KeyValue:
    key: str
    value: str


@dataclass
class WrittenParquetPath:
    """Represent a single object written to storage in a partitioned path."""

    root: SinkOutputRootPath
    # TODO: see if we can avoid having basename here and think of a different class name.
    basename: str
    partitions: list[KeyValue]
    row_count: int

    @classmethod
    def from_partition(
        cls, root: SinkOutputRootPath, basename: str, partitions: list[KeyValue], row_count: int
    ) -> "WrittenParquetPath":
        return cls(
            root=root,
            basename=basename,
            partitions=partitions,
            row_count=row_count,
        )

    @property
    def partitions_path(self):
        return "/".join(f"{col.key}={col.value}" for col in self.partitions)

    @property
    def full_path(self):
        return os.path.join(self.root, self.partitions_path, self.basename)
