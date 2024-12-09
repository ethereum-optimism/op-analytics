import os
from enum import Enum

from op_analytics.coreutils.path import repo_path
from op_analytics.coreutils.env.aware import is_bot


class DataLocation(str, Enum):
    """Supported storage locations for partitioned data."""

    DISABLED = "DISABLED"
    GCS = "GCS"
    LOCAL = "LOCAL"
    BIGQUERY = "BIGQUERY"
    BIGQUERY_LOCAL_MARKERS = "BIGQUERY_LOCAL_MARKERS"

    def __repr__(self):
        return f"DataLocation.{self.name}"

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

    def check_write_allowed(self):
        if self == DataLocation.GCS and not is_bot():
            if os.environ.get("ALLOW_WRITE") == "true":
                return

            raise Exception("GCS data can only be written from a bot runtime.")

        return


class MarkersLocation(str, Enum):
    """Supported storage locations for markers of partitioned data."""

    OPLABS_CLICKHOUSE = "OPLABS_CLICKHOUSE"
    DUCKDB_LOCAL = "DUCKDB_LOCAL"
