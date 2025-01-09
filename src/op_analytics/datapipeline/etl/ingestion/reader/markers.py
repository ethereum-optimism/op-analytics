from dataclasses import dataclass, field
from datetime import date

import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dataaccess import init_data_access
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.datapipeline.chains.testnets import TestnetRootPathAdapter

log = structlog.get_logger()

INGESTION_MARKERS_TABLE = "raw_onchain_ingestion_markers"

DEFAULT_INGESTION_ROOT_PATHS = [
    "ingestion/blocks_v1",
    "ingestion/transactions_v1",
    "ingestion/logs_v1",
    "ingestion/traces_v1",
]


@dataclass
class IngestionData:
    # True if the data is complete and ready to be consumed.
    is_complete: bool

    # Physical parquet paths (values) that will be read for each logical root path (keys).
    data_paths: dict[str, list[str]] | None


@dataclass
class IngestionDataSpec:
    # Chains that will be read.
    chains: list[str]

    # Root paths that will be read. Logical names which may be different
    # than physical names where the data is actualy stored.
    root_paths_to_read: list[str] | None = None

    adapter: TestnetRootPathAdapter = field(init=False)

    def __post_init__(self):
        self.adapter = TestnetRootPathAdapter(
            chains=self.chains,
            root_path_prefix="ingestion",
            root_paths_to_read=self.root_paths_to_read or DEFAULT_INGESTION_ROOT_PATHS,
        )

    def query_markers(
        self,
        datevals: list[date],
        location: DataLocation,
    ) -> pl.DataFrame:
        """Query completion markers for a list of dates and chains.

        Returns a dataframe with the markers and all of the parquet output paths
        associated with them.
        """
        client = init_data_access()

        return client.query_markers_by_root_path(
            chains=self.chains,
            datevals=datevals,
            data_location=location,
            root_paths=self.adapter.root_paths_query_filter(),
            markers_table=INGESTION_MARKERS_TABLE,
            extra_columns=[
                "num_blocks",
                "min_block",
                "max_block",
            ],
        )
