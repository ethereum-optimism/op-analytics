from dataclasses import dataclass
from typing import Any

import polars as pl


@dataclass
class ExpectedOutput:
    """Information about a dataset that is expectd to be produced by a task."""

    # Root path that will be used for the partitioned output.
    root_path: str

    # File name that will be used for the parquet file.
    file_name: str

    # Completion marker path.
    marker_path: str


@dataclass
class OutputData:
    dataframe: pl.DataFrame

    # Root path
    root_path: str

    # Default partition values for cases when the output dataframe is empty
    # and therefore has no implicit partition values.
    default_partitions: list[dict[str, Any]] | None = None
