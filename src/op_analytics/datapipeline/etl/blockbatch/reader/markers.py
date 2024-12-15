from dataclasses import dataclass
from datetime import date

import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dataaccess import init_data_access, DateFilter, MarkerFilter
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.datapipeline.chains.testnets import TestnetRootPathAdapter
from op_analytics.datapipeline.models.compute.model import PythonModel

log = structlog.get_logger()


BLOCKBATCH_MARKERS_TABLE = "blockbatch_model_markers"

BLOCKBATCH_MARKERS_QUERY_SCHEMA = {
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
class BlockBatchDataSpec:
    # Chains that will be read.
    chains: list[str]

    models: list[str]

    def __post_init__(self):
        root_paths_to_read = []
        for _ in self.models:
            model: PythonModel = PythonModel.get(_)
            for output_dataset in model.expected_output_datasets:
                root_paths_to_read.append(f"blockbatch/{model.name}/{output_dataset}")

        self.adapter = TestnetRootPathAdapter(
            chains=self.chains,
            root_paths_to_read=root_paths_to_read,
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
            markers_table=BLOCKBATCH_MARKERS_TABLE,
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

        assert dict(paths_df.schema) == BLOCKBATCH_MARKERS_QUERY_SCHEMA

        return paths_df
