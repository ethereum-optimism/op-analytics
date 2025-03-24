from dataclasses import dataclass
from typing import Any

from op_analytics.coreutils.duckdb_inmem import RemoteParquetData
from op_analytics.coreutils.logger import structlog

from .location import DataLocation
from .partition import Partition

log = structlog.get_logger()


@dataclass
class DataReader:
    """Manages reading partitioned data.

    Consumers must know if the data is ready to use (this class does not check
    markers).

    If data is incomplete any current parquet paths in GCS will be returned. It's
    up to the consumer to decide what to do with them.

    On some cases it will be preferable to do no processing for incomplete data.

    On other cases it can make sense to do partial processing (e.g. when loading
    data into BigQuery).
    """

    partitions: Partition

    # Source
    read_from: DataLocation

    # Input data as parquet paths for each dataset.
    dataset_paths: dict[str, list[str]]
    inputs_ready: bool

    # Additional columns stored in the marker table.
    # The dictionary is column name to column value.
    extra_marker_data: dict[str, Any] | None = None

    def marker_data(self, key: str) -> Any:
        """Get marker data for a key.

        Fails if no marker data or key not present.
        Use when the data is strictly required.
        """
        assert self.extra_marker_data is not None
        return self.extra_marker_data[key]

    def get_marker_data(self, key: str) -> Any:
        """Get marker data for a key.

        Does not fail if no marker data or key not present.
        Use for logging.
        """
        return (self.extra_marker_data or {}).get(key)

    def partitions_dict(self):
        return self.partitions.as_dict()

    def debugging_context(self):
        """Return a dictionary with partition and marker data for debugging."""
        return {
            **self.partitions_dict(),
            **(self.extra_marker_data or {}),
        }

    @property
    def paths_summary(self) -> dict[str, int]:
        return {dataset_name: len(paths) for dataset_name, paths in self.dataset_paths.items()}

    def partition_value(self, column_name: str) -> str:
        return self.partitions.column_value(column_name)

    def get_parquet_paths(
        self, dataset: str, first_n_parquet_files: int | None = None
    ) -> list[str]:
        paths = sorted(self.dataset_paths[dataset])
        num_paths = len(paths)

        if first_n_parquet_files is not None:
            paths = paths[:first_n_parquet_files]

        num_used_paths = len(paths)

        log.info(
            f"reading dataset={dataset!r} using {num_used_paths}/{num_paths} parquet paths, first path is {paths[0]}"
        )

        return paths

    def remote_parquet(
        self,
        dataset: str,
        first_n_parquet_files: int | None = None,
    ) -> RemoteParquetData:
        return RemoteParquetData.for_dataset(
            dataset=dataset,
            parquet_paths=self.get_parquet_paths(dataset, first_n_parquet_files),
        )
