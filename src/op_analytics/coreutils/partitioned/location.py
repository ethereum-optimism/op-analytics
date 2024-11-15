import os
from enum import Enum

from op_analytics.coreutils.path import repo_path


class DataLocation(str, Enum):
    """Supported storage locations for partitioned data."""

    GCS = "GCS"
    LOCAL = "LOCAL"
    BIGQUERY = "BIGQUERY"
    BIGQUERY_LOCAL_MARKERS = "BIGQUERY_LOCAL_MARKERS"

    def with_prefix(self, path: str) -> str:
        if self == DataLocation.GCS:
            # Prepend the GCS bucket scheme and bucket name to make the paths
            # understandable by read_parquet() in DuckDB.
            return f"gs://oplabs-tools-data-sink/{path}"

        if self == DataLocation.LOCAL:
            # Prepend the default loal path.
            return os.path.join("ozone/warehouse", path)

        raise NotImplementedError()

    def absolute(self, path: str) -> str:
        if self == DataLocation.GCS:
            return self.with_prefix(path)
        if self == DataLocation.LOCAL:
            local_path = repo_path(self.with_prefix(path))
            if local_path is None:
                raise RuntimeError(f"could not determine local path: {self.with_prefix(path)}")
            return os.path.abspath(local_path)

        raise NotImplementedError()

    def ensure_biguqery(self):
        if self not in (DataLocation.BIGQUERY, DataLocation.BIGQUERY_LOCAL_MARKERS):
            raise ValueError(f"invalid location for bigquery load: {self}")


class MarkersLocation(str, Enum):
    """Supported storage locations for markers of partitioned data."""

    OPLABS_CLICKHOUSE = "OPLABS_CLICKHOUSE"
    DUCKDB_LOCAL = "DUCKDB_LOCAL"


def marker_location(data_location: DataLocation) -> MarkersLocation:
    """Location of markers for a given DataLocation.

    - GCS markers go to ClickHouse
    - LOCAL markers to got local DuckDB
    """
    if data_location == DataLocation.GCS:
        return MarkersLocation.OPLABS_CLICKHOUSE

    if data_location == DataLocation.LOCAL:
        return MarkersLocation.DUCKDB_LOCAL

    if data_location == DataLocation.BIGQUERY:
        return MarkersLocation.OPLABS_CLICKHOUSE

    if data_location == DataLocation.BIGQUERY_LOCAL_MARKERS:
        return MarkersLocation.DUCKDB_LOCAL

    raise NotImplementedError(f"invalid data location: {data_location}")
