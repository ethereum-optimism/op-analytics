from dataclasses import dataclass, field
from datetime import date

import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dataaccess import DateFilter, MarkerFilter, init_data_access
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

INGESTION_MARKERS_QUERY_SCHEMA = {
    "dt": pl.Date,
    "chain": pl.String,
    "marker_path": pl.String,
    "num_parts": pl.UInt32,
    "num_blocks": pl.Int32,
    "min_block": pl.Int64,
    "max_block": pl.Int64,
    "root_path": pl.String,
    "data_path": pl.String,
}


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
            root_paths_to_read=self.root_paths_to_read or DEFAULT_INGESTION_ROOT_PATHS,
        )

    def query_markers(
        self,
        datevals: list[date],
        read_from: DataLocation,
    ) -> pl.DataFrame:
        """Query completion markers for a list of dates and chains.

        Returns a dataframe with the markers and all of the parquet output paths
        associated with them.
        """
        # Make one query for all dates and chains.
        #
        # We use the +/- 1 day padded dates so that we can use the query results to
        # check if there is data on boths ends. This allows us to confirm that the
        # data is ready to be processed.
        client = init_data_access()

        paths_df = client.markers_for_dates(
            data_location=read_from,
            markers_table=INGESTION_MARKERS_TABLE,
            datefilter=DateFilter(
                min_date=None,
                max_date=None,
                datevals=datevals,
            ),
            projections=[
                "dt",
                "chain",
                "marker_path",
                "num_parts",
                "num_blocks",
                "min_block",
                "max_block",
                "data_path",
                "root_path",
            ],
            filters={
                "chains": MarkerFilter(
                    column="chain",
                    values=self.chains,
                ),
                "datasets": MarkerFilter(
                    column="root_path",
                    values=self.adapter.root_paths_query_filter(),
                ),
            },
        )

        assert dict(paths_df.schema) == INGESTION_MARKERS_QUERY_SCHEMA

        return paths_df
