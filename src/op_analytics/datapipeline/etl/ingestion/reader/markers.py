from dataclasses import dataclass
from datetime import date

import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dataaccess import init_data_access
from op_analytics.coreutils.partitioned.location import DataLocation

from .rootpaths import RootPath

log = structlog.get_logger()

INGESTION_MARKERS_TABLE = "raw_onchain_ingestion_markers"


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
    root_paths_to_read: list[RootPath]

    def physical_root_paths(self) -> list[str]:
        physical_root_paths = set()
        for chain in self.chains:
            for root_path in self.root_paths_to_read:
                physical_root_paths.add(root_path.physical_for_chain(chain))
        return sorted(physical_root_paths)

    def physical_root_paths_for_chain(self, chain: str) -> list[str]:
        physical_root_paths = set()
        for root_path in self.root_paths_to_read:
            physical_root_paths.add(root_path.physical_for_chain(chain))
        return sorted(physical_root_paths)

    def data_paths_keyed_by_logical_path(
        self,
        chain: str,
        physical_paths: dict[str, list[str]] | None,
    ):
        # Root paths in `markers_df`` are all physical. This means that the
        # `input_data.data_paths`` dictionary will have physical paths as keys.
        # Here we remap the physical paths to logical paths so that data readers
        # can continue to operate on logical paths. i.e. given a logical path
        # key they get back the physical dataset paths where the data is stored.
        dataset_paths: dict[str, list[str]] = {}
        if physical_paths is not None:
            for root_path in self.root_paths_to_read:
                dataset_paths[root_path.root_path] = physical_paths[
                    root_path.physical_for_chain(chain)
                ]
        return dataset_paths

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
            markers_table=INGESTION_MARKERS_TABLE,
            extra_columns=[
                "num_blocks",
                "min_block",
                "max_block",
            ],
        )
