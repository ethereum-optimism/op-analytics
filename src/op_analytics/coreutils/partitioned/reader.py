from dataclasses import dataclass


from op_analytics.coreutils.duckdb_inmem import register_parquet_relation
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

    On some cases it will be preferable to do no processing for incomplete data
    (e.g. when processing intermediate models).

    On other cases it can make sense to do partial processing (e.g. when loading
    data into BigQuery).
    """

    partitions: Partition

    # Source
    read_from: DataLocation

    # Input data as parquet paths for each dataset.
    dataset_paths: dict[str, list[str]]
    inputs_ready: bool

    def partitions_dict(self):
        return self.partitions.as_dict()

    @property
    def paths_summary(self) -> dict[str, int]:
        return {dataset_name: len(paths) for dataset_name, paths in self.dataset_paths.items()}

    def partition_value(self, column_name: str) -> str:
        return self.partitions.column_value(column_name)

    def register_duckdb_relation(
        self,
        dataset,
        first_n_parquet_files: int | None = None,
    ) -> str:
        paths = sorted(self.dataset_paths[dataset])
        num_paths = len(paths)

        if first_n_parquet_files is not None:
            paths = paths[:first_n_parquet_files]

        num_used_paths = len(paths)

        log.info(
            f"duckdb dataset={dataset!r} using {num_used_paths}/{num_paths} parquet paths, first path is {paths[0]}"
        )

        return register_parquet_relation(
            dataset=dataset,
            parquet_paths=paths,
        )
