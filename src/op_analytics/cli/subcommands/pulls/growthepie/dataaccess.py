from enum import Enum

import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydata import read_daily_data, write_daily_data
from op_analytics.coreutils.partitioned.location import DataLocation

log = structlog.get_logger()


class GrowThePie(str, Enum):
    """Supported growthepie datasets.

    This class includes utilities to read data from each dataset from a notebook
    for ad-hoc use cases.
    """

    # L2 Chain Fundamentals
    FUNDAMENTALS_SUMMARY = "chains_daily_fundamentals_v1"

    # Metadata for the chains
    CHAIN_METADATA = "chains_metadata_v1"

    # Contract labels
    CONTRACT_LABELS = "gtp_contract_labels_v1"

    @property
    def root_path(self):
        return f"growthepie/{self.value}"

    def write(
        self,
        dataframe: pl.DataFrame,
        sort_by: list[str] | None = None,
    ):
        return write_daily_data(
            root_path=self.root_path,
            dataframe=dataframe,
            sort_by=sort_by,
        )

    def read(
        self,
        min_date: str | None = None,
        max_date: str | None = None,
        date_range_spec: str | None = None,
    ) -> str:
        """Read growthepie data. Optionally filtered by date."""
        return read_daily_data(
            root_path=self.root_path,
            min_date=min_date,
            max_date=max_date,
            date_range_spec=date_range_spec,
            location=DataLocation.GCS,
        )
