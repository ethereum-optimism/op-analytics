import os
import re
from dataclasses import dataclass
from datetime import date

import polars as pl

from op_analytics.coreutils.time import date_fromstr


DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


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
class Partition:
    """All the partition columns for a specific partition in a dataset."""

    cols: list[PartitionColumn]

    def __hash__(self):
        return hash(self.path)

    def __iter__(self):
        return iter(self.cols)

    @classmethod
    def from_tuples(cls, partitions: list[tuple[str, str]]):
        return cls(cols=[PartitionColumn(name=k, value=v) for k, v in partitions])

    def as_dict(self):
        return {col.name: col.value for col in self.cols}

    @property
    def path(self):
        return "/".join(f"{col.name}={col.value}" for col in self.cols)

    def column_value(self, column_name: str) -> str:
        for partition in self.cols:
            if partition.name == column_name:
                return partition.value
        raise ValueError(f"partition not found: {column_name}")

    def full_path(self, root_path: str, file_name: str):
        """Produce the full path for this expected output.

        The full path is a combination of:

        - root_path   ex: ingestion
        - partitions  ex: chain=op/dt=2024-11-01
        - file name   ex: 00001000.parquet

        Full path:

        ingestion/chain=op/dt=2024-11-01/00001000.parquet
        """
        return os.path.join(root_path, self.path, file_name)


@dataclass
class PartitionData:
    """DataFrame data for a given partition."""

    partition: Partition
    df: pl.DataFrame

    def partition_value(self, partition_name: str) -> str:
        return self.partition.column_value(partition_name)

    @classmethod
    def from_dict(
        cls,
        partition_cols: list[str],
        partitions_dict: dict[str, str],
        df: pl.DataFrame,
    ) -> "PartitionData":
        cols = []
        for col in partition_cols:
            val = partitions_dict[col]
            # NOTE: Here we validate the type of the "dt" column.
            if col == "dt" and not isinstance(val, (date, str)):
                raise ValueError(
                    f"dt partition in dataframe is invalid: must be date or str: {val}"
                )

            # NOTE: The call to PartitionColumn validates the "dt" value.
            if col == "dt" and isinstance(val, date):
                cols.append(PartitionColumn(name=col, value=val.strftime("%Y-%m-%d")))
            else:
                cols.append(PartitionColumn(name=col, value=val))

        return cls(partition=Partition(cols), df=df)


@dataclass
class PartitionMetadata:
    row_count: int | None = None


type WrittenParts = dict[Partition, PartitionMetadata]
