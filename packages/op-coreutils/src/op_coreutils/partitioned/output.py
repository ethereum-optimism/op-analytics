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

    # Root path that will be used for the partitioned output.
    root_path: SinkOutputRootPath

    # File name that will be used for the parquet file.
    file_name: str

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

    # Name of the datset.
    dataset_name: str

    # Default partition values for cases when the output datafarame is empty
    # and therefore has no implicit partition values.
    default_partition: dict[str, Any] | None


@dataclass
class KeyValue:
    key: str
    value: str


@dataclass
class OutputPartMeta:
    """Metadata for an output part."""

    partitions: list[KeyValue]
    row_count: int

    @property
    def partitions_path(self):
        return "/".join(f"{col.key}={col.value}" for col in self.partitions)

    def full_path(self, root_path: str, file_name: str):
        return os.path.join(root_path, self.partitions_path, file_name)


@dataclass
class OutputPart:
    """Data and metadadta for a single part in a a partitioned output."""

    df: pl.DataFrame
    meta: OutputPartMeta
