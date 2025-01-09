from dataclasses import dataclass, field
from datetime import date

import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dataaccess import init_data_access
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.datapipeline.models.compute.model import PythonModel
from op_analytics.datapipeline.etl.ingestion.reader.rootpaths import RootPath

log = structlog.get_logger()


@dataclass
class ModelsDataSpec:
    chains: list[str]
    models: list[str]
    markers_table: str
    root_path_prefix: str

    root_paths_to_read: list[RootPath] = field(init=False, default_factory=list)

    def __post_init__(self):
        for _ in self.models:
            model: PythonModel = PythonModel.get(_)
            for output_dataset in model.expected_output_datasets:
                self.root_paths_to_read.append(
                    RootPath.of(root_path=f"{self.root_path_prefix}/{model.name}/{output_dataset}")
                )

    def physical_root_paths(self) -> list[str]:
        physical_root_paths = set()
        for chain in self.chains:
            for root_path in self.root_paths_to_read:
                physical_root_paths.add(root_path.physical_for_chain(chain))
        return sorted(physical_root_paths)

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
            root_paths=self.physical_root_paths(),
            markers_table=self.markers_table,
            extra_columns=[],
        )
