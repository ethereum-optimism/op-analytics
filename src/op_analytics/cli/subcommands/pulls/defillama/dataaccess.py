import glob
import os
from enum import Enum
from functools import cache

import polars as pl

from op_analytics.coreutils.duckdb_inmem import init_client
from op_analytics.coreutils.env.aware import OPLabsEnvironment, current_environment
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydata import read_daily_data, write_daily_data
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.coreutils.path import repo_path
from op_analytics.coreutils.time import now_friendly_timestamp

log = structlog.get_logger()


class DefiLlama(str, Enum):
    """Supported defillama datasets.

    This class includes utilities to read data from each dataset from a notebook
    for ad-hoc use cases.
    """

    CHAINS_METADATA = "chains_metadata_v1"

    HISTORICAL_CHAIN_TVL = "historical_chain_tvl_v1"

    PROTOCOLS_METADATA = "protocols_metadata_v1"
    PROTOCOLS_TVL = "protocols_tvl_v1"
    PROTOCOLS_TOKEN_TVL = "protocols_token_tvl_v1"

    @property
    def root_path(self):
        return f"defillama/{self.value}"

    def write(
        self,
        dataframe: pl.DataFrame,
        sort_by: list[str] | None = None,
    ):
        return write_daily_data(
            location=write_location(),
            root_path=self.root_path,
            dataframe=dataframe,
            sort_by=sort_by,
            # Always overwrite data. If we pull data in early for a given date
            # a subsequent data pull will overwrite with more complete data.
            force_complete=True,
        )

    def read(
        self,
        min_date: str | None = None,
        max_date: str | None = None,
        date_range_spec: str | None = None,
    ):
        """Read defillama data. Optionally filtered by date."""
        return read_daily_data(
            root_path=self.root_path,
            min_date=min_date,
            max_date=max_date,
            date_range_spec=date_range_spec,
            location=DataLocation.GCS,
        )

    def dump_local_copy(
        self,
        min_date: str | None = None,
        max_date: str | None = None,
        date_range_spec: str | None = None,
    ):
        """Dump a copy of the data to a single local parquet file."""
        client = self.read(min_date, max_date, date_range_spec)

        path = self._local_path()
        log.info(f"begin dump copy to {path!r}")
        client.view(self.value).write_parquet(path, compression="zstd")
        log.info(f"done dump copy to {path!r}")
        self._register_local_copy(path)

    def load_local_copy(self, timestamp: str | None = None):
        """Load a local parquet file.

        If timestamp is not provided the last created local copy will be loaded.
        """

        dirname = os.path.dirname(self._local_path())
        if timestamp is None:
            candidates = sorted(
                glob.glob(os.path.join(dirname, f"{self.value}__*.parquet")), reverse=True
            )
            if not candidates:
                raise Exception(f"no local copies found for {self.value!r}")
            path = candidates[0]
        else:
            path = os.path.join(dirname, f"{self.value}__{timestamp}.parquet")

        log.info(f"load copy from {path!r}")
        return self._register_local_copy(path)

    def _register_local_copy(self, path: str):
        client = init_client()

        client.register(
            view_name=os.path.basename(path).removesuffix(".parquet"),
            python_object=client.read_parquet(path),
        )

        print(client.sql("SHOW TABLES"))
        return client

    def _local_path(self):
        path = repo_path(f"parquet/defillama/{self.value}__{now_friendly_timestamp()}.parquet")
        assert path is not None
        os.makedirs(os.path.dirname(path), exist_ok=True)
        return path


@cache
def write_location():
    if current_environment() == OPLabsEnvironment.UNITTEST:
        return DataLocation.LOCAL
    else:
        return DataLocation.GCS
