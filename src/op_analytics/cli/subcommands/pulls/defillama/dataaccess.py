from enum import Enum

import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydata import read_daily_data, write_daily_data
from op_analytics.coreutils.partitioned.location import DataLocation

log = structlog.get_logger()


class DefiLlama(str, Enum):
    """Supported defillama datasets.

    This class includes utilities to read data from each dataset from a notebook
    for ad-hoc use cases.
    """

    # Chain TVL
    CHAINS_METADATA = "chains_metadata_v1"
    HISTORICAL_CHAIN_TVL = "historical_chain_tvl_v1"

    # Protocol TVL
    PROTOCOLS_METADATA = "protocols_metadata_v1"
    PROTOCOLS_TVL = "protocols_tvl_v1"
    PROTOCOLS_TOKEN_TVL = "protocols_token_tvl_v1"

    # Stablecoins TVL
    STABLECOINS_METADATA = "stablecoins_metadata_v1"
    STABLECOINS_BALANCE = "stablecoins_balances_v1"

    @property
    def root_path(self):
        return f"defillama/{self.value}"

    def write(
        self,
        dataframe: pl.DataFrame,
        sort_by: list[str] | None = None,
    ):
        return write_daily_data(
            root_path=self.root_path,
            dataframe=dataframe,
            sort_by=sort_by,
            # Override the location value here. To write to the local file system
            # use DataLocation.LOCAL
            location=DataLocation.GCS,
        )

    def read(
        self,
        min_date: str | None = None,
        max_date: str | None = None,
        date_range_spec: str | None = None,
    ) -> str:
        """Read defillama data. Optionally filtered by date."""
        return read_daily_data(
            root_path=self.root_path,
            min_date=min_date,
            max_date=max_date,
            date_range_spec=date_range_spec,
            location=DataLocation.GCS,
        )
