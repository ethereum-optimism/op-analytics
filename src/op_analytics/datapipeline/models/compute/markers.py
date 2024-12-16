from dataclasses import dataclass
from datetime import date

import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dataaccess import init_data_access
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.datapipeline.chains.testnets import TestnetRootPathAdapter
from op_analytics.datapipeline.models.compute.model import PythonModel

log = structlog.get_logger()


INTERMEDIATE_MARKERS_TABLE = "intermediate_model_markers"


@dataclass
class ModelsDataSpec:
    chains: list[str]
    models: list[str]
    markers_table: str
    root_path_prefix: str

    def __post_init__(self):
        root_paths_to_read = []
        for _ in self.models:
            model: PythonModel = PythonModel.get(_)
            for output_dataset in model.expected_output_datasets:
                root_paths_to_read.append(f"{self.root_path_prefix}/{model.name}/{output_dataset}")

        self.adapter = TestnetRootPathAdapter(
            chains=self.chains,
            root_path_prefix=self.root_path_prefix,
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
        client = init_data_access()

        return client.query_markers_by_root_path(
            chains=self.chains,
            datevals=datevals,
            data_location=read_from,
            root_paths=self.adapter.root_paths_query_filter(),
            markers_table=INTERMEDIATE_MARKERS_TABLE,
            extra_columns=[],
        )
