import os
from dataclasses import dataclass
from typing import Any

import polars as pl
import pyarrow as pa

from .partition import Partition


@dataclass
class ExpectedOutput:
    """Information about a dataset that is expectd to be produced by a task."""

    # Root path that will be used for the partitioned output.
    root_path: str

    # File name that will be used for the parquet file.
    file_name: str

    # Completion marker path.
    marker_path: str

    # Identifier for the process that produced the datset.
    process_name: str

    # Values for additional columns stored in the markers table.
    additional_columns: dict[str, Any]

    # Schema for additional columns stored in the markers table.
    # This schema is used to create a pyarrow table to write markers
    # into the markers table.
    additional_columns_schema: list[pa.Field]

    def full_path(self, partition: Partition):
        """Produce the full path for this expected output.

        The full path is a combination of:

        - root_path   ex: ingestion
        - partitions  ex: chain=op/dt=2024-11-01
        - file name   ex: 00001000.parquet

        Full path:

        ingestion/chain=op/dt=2024-11-01/00001000.parquet
        """
        return os.path.join(self.root_path, partition.path, self.file_name)


@dataclass
class OutputData:
    dataframe: pl.DataFrame

    # Root path
    root_path: str

    # Default partition values for cases when the output datafarame is empty
    # and therefore has no implicit partition values.
    default_partition: dict[str, Any] | None
