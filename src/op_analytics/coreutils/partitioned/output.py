import os
import re
from dataclasses import dataclass
from typing import Any

import polars as pl
import pyarrow as pa

from op_analytics.coreutils.time import date_fromstr

from .types import PartitionedMarkerPath, PartitionedRootPath


@dataclass
class PartitionColumn:
    """The name and value of a single partition column."""

    name: str
    value: str

    def __post_init__(self):
        if not isinstance(self.value, str):
            raise ValueError(f"partition value must be a string: {self.value!r}")
        if not isinstance(self.name, str):
            raise ValueError(f"partition key must be a string: {self.name!r}")

        if self.name == "dt":
            if not DATE_RE.match(self.value):
                raise ValueError(f"partition value must be a date pattern: {self.value!r}")

            try:
                date_fromstr(self.value)
            except Exception as ex:
                raise ValueError(f"partition value must be a valid date: {self.value!r}") from ex


@dataclass
class PartitionColumns:
    """All the partition columns for a specific partition in a dataset."""

    cols: list[PartitionColumn]

    def __iter__(self):
        return iter(self.cols)

    @property
    def path(self):
        return "/".join(f"{col.name}={col.value}" for col in self.cols)

    def column_value(self, column_name: str) -> str:
        for partition in self.cols:
            if partition.name == column_name:
                return partition.value
        raise ValueError(f"partition not found: {column_name}")


@dataclass
class ExpectedOutput:
    """Information about a dataset that is expectd to be produced by a task."""

    # Name of the datset.
    dataset_name: str

    # Root path that will be used for the partitioned output.
    root_path: PartitionedRootPath

    # File name that will be used for the parquet file.
    file_name: str

    # Completion marker path.
    marker_path: PartitionedMarkerPath

    # Identifier for the process that produced the datset.
    process_name: str

    # Values for additional columns stored in the markers table.
    additional_columns: dict[str, Any]

    # Schema for additional columns stored in the markers table.
    # This schema is used to create a pyarrow table to write markers
    # into the markers table.
    additional_columns_schema: list[pa.Field]

    def full_path(self, partitions: PartitionColumns):
        """Produce the full path for this expected output.

        The full path is a combination of:

        - root_path   ex: ingestion
        - partitions  ex: chain=op/dt=2024-11-01
        - file name   ex: 00001000.parquet

        Full path:

        ingestion/chain=op/dt=2024-11-01/00001000.parquet
        """
        return os.path.join(self.root_path, partitions.path, self.file_name)


DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@dataclass
class OutputData:
    dataframe: pl.DataFrame

    # Name of the datset.
    dataset_name: str

    # Default partition values for cases when the output datafarame is empty
    # and therefore has no implicit partition values.
    default_partition: dict[str, Any] | None


@dataclass
class OutputPartMeta:
    """Metadata for an output part."""

    partitions: PartitionColumns
    row_count: int

    @classmethod
    def from_tuples(cls, partitions: list[tuple[str, str]], row_count: int):
        return cls(
            partitions=PartitionColumns([PartitionColumn(name=k, value=v) for k, v in partitions]),
            row_count=row_count,
        )


@dataclass
class PartitionData:
    """The dataframe with all the data for a given partition."""

    partitions: PartitionColumns
    df: pl.DataFrame

    def partition_value(self, partition_name: str) -> str:
        return self.partitions.column_value(partition_name)

    @property
    def meta(self):
        return OutputPartMeta(
            partitions=self.partitions,
            row_count=len(self.df),
        )

    @classmethod
    def from_dict(cls, partitions_dict: dict[str, str], df: pl.DataFrame) -> "PartitionData":
        return cls(
            partitions=PartitionColumns(
                [PartitionColumn(name=col, value=val) for col, val in partitions_dict.items()]
            ),
            df=df,
        )
