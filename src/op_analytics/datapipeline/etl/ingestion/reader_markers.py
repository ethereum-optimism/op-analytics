from dataclasses import dataclass, field
from datetime import date

import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dataaccess import init_data_access, DateFilter, MarkerFilter
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.datapipeline.chains.goldsky_chains import ensure_single_network, ChainNetwork
from op_analytics.coreutils.time import date_fromstr

from .markers import INGESTION_MARKERS_TABLE

log = structlog.get_logger()


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
    root_paths_to_read: list[str]

    # Mapping from actual physical paths that will be read to logical paths
    # to read. Physical paths can be different for TESTNET chains.
    root_path_mapping: dict[str, str] = field(init=False)

    # List of physical root paths that are checked and read from storage.
    root_paths_physical: list[str] = field(init=False)

    def __post_init__(self):
        self.root_path_mapping = update_root_paths(self.chains, self.root_paths_to_read)
        self.root_paths_physical = sorted(self.root_path_mapping.keys())

    def query_markers(
        self,
        datevals: list[date],
        read_from: DataLocation,
    ) -> pl.DataFrame:
        # Make one query for all dates and chains.
        #
        # We use the +/- 1 day padded dates so that we can use the query results to
        # check if there is data on boths ends. This allows us to confirm that the
        # data is ready to be processed.
        markers_df = markers_for_raw_ingestion(
            data_location=read_from,
            markers_table=INGESTION_MARKERS_TABLE,
            datevals=datevals,
            chains=self.chains,
            root_paths=self.root_paths_physical,
        )

        return markers_df

    def data_paths(self, physical_data_paths: dict[str, list[str]] | None) -> dict[str, list[str]]:
        """Updates keys to be logical pahts.

        The input dictionary is a map from physical root path to physical data paths.
        The output dictionary replaces the keys with logical root paths.
        """
        updated_dataset_paths = {}
        for root_path, data_paths in (physical_data_paths or {}).items():
            updated_dataset_paths[self.root_path_mapping[root_path]] = data_paths
        return updated_dataset_paths


def markers_for_raw_ingestion(
    data_location: DataLocation,
    markers_table: str,
    datevals: list[date],
    chains: list[str],
    root_paths: list[str],
) -> pl.DataFrame:
    """Query completion markers for a list of dates and chains.

    Returns a dataframe with the markers and all of the parquet output paths
    associated with them.
    """
    client = init_data_access()

    paths_df = client.markers_for_dates(
        data_location=data_location,
        markers_table=markers_table,
        datefilter=DateFilter(
            min_date=None,
            max_date=None,
            datevals=datevals,
        ),
        projections=[
            "marker_path",
            "dt",
            "chain",
            "num_blocks",
            "min_block",
            "max_block",
            "data_path",
            "root_path",
        ],
        filters={
            "chains": MarkerFilter(
                column="chain",
                values=chains,
            ),
            "datasets": MarkerFilter(
                column="root_path",
                values=root_paths,
            ),
        },
    )

    assert dict(paths_df.schema) == {
        "marker_path": pl.String,
        "dt": pl.Date,
        "chain": pl.String,
        "num_blocks": pl.Int32,
        "min_block": pl.Int64,
        "max_block": pl.Int64,
        "root_path": pl.String,
        "data_path": pl.String,
    }

    return paths_df


def update_root_paths(chains: list[str], root_paths: list[str]) -> dict[str, str]:
    """Update root paths to account for TESTNET network.

    Returns a dictionary where keys are the updated root_paths and values are the
    original root_paths.

    Root paths are different for mainnet and testnet:

      MAINNET  :  ingestion/
      TESTNET :  ingestion_testnets/

    We update the root paths to account for TESTNET chains, keeping a reference to the
    original root_path that was requested to the reader.

    This lets model implementations always refer to data with the mainnet path, for example
    "ingestion/traces_v1". When running on testnet we use the correct location for the data.
    """
    network = ensure_single_network(chains)
    updated_root_paths = {}
    if network == ChainNetwork.TESTNET:
        for path in root_paths:
            if path.startswith("ingestion/"):
                updated_root_paths["ingestion_testnets/" + path.removeprefix("ingestion/")] = path
            else:
                updated_root_paths[path] = path
    else:
        for path in root_paths:
            updated_root_paths[path] = path

    return updated_root_paths


CHAIN_ACTIVATION_DATES = {
    "automata": date_fromstr("2024-07-17"),
    "base": date_fromstr("2023-06-15"),
    "bob": date_fromstr("2024-04-11"),
    "cyber": date_fromstr("2024-04-18"),
    "fraxtal": date_fromstr("2024-02-01"),
    "ham": date_fromstr("2024-05-24"),
    "kroma": date_fromstr("2023-09-05"),
    "lisk": date_fromstr("2024-05-03"),
    "lyra": date_fromstr("2023-11-15"),
    "metal": date_fromstr("2024-03-27"),
    "mint": date_fromstr("2024-05-13"),
    "mode": date_fromstr("2023-11-16"),
    "op": date_fromstr("2021-11-12"),
    "orderly": date_fromstr("2023-10-06"),
    "polynomial": date_fromstr("2024-06-10"),
    "race": date_fromstr("2024-07-08"),
    "redstone": date_fromstr("2024-04-03"),
    "shape": date_fromstr("2024-07-23"),
    "swan": date_fromstr("2024-06-18"),
    "unichain": date_fromstr("2024-11-04"),
    "worldchain": date_fromstr("2024-06-25"),
    "xterio": date_fromstr("2024-05-24"),
    "zora": date_fromstr("2023-06-13"),
    # TESTNETS
    "op_sepolia": date_fromstr("2024-01-01"),
    "unichain_sepolia": date_fromstr("2024-09-19"),
}


def is_chain_active(chain: str, dateval: date) -> bool:
    activation = CHAIN_ACTIVATION_DATES[chain]

    return dateval >= activation
