from dataclasses import dataclass

import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dataaccess import init_data_access
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.coreutils.rangeutils.blockrange import BlockRange, ChainMaxBlock
from op_analytics.coreutils.rangeutils.timerange import TimeRange

from .ranges import get_chain_block_ranges, get_chain_max_blocks, time_range_for_blocks
from .rootpaths import RootPath

log = structlog.get_logger()


BLOCKBATCH_MARKERS_TABLE = "blockbatch_markers"


@dataclass
class BlockBatchRequest:
    # Chains requested
    chains: list[str]

    # The block range requested for each chain.
    chain_block_ranges: dict[str, BlockRange]

    # The max block for each chain.
    chain_max_blocks: dict[str, ChainMaxBlock]

    # The time range associated with this request
    time_range: TimeRange

    # Root paths that will be read. Logical names which may be different
    # than physical names where the data is actualy stored.
    root_paths_to_read: list[RootPath]

    # If the request is for a specific range of blocks.
    block_range: BlockRange | None = None

    @classmethod
    def build(
        cls,
        chains: list[str],
        range_spec: str,
        root_paths_to_read: list[RootPath],
    ) -> "BlockBatchRequest":
        block_range: BlockRange | None
        try:
            block_range = BlockRange.from_spec(range_spec)

            if len(chains) != 1:
                raise Exception(
                    "Ingesting by block_range is only supported for one chain at a time."
                )

            chain_block_ranges = {}
            for chain in chains:
                chain_block_ranges[chain] = block_range

            # Determine the time range for the provided block range.
            time_range = time_range_for_blocks(
                chain=chains[0],
                min_block=block_range.min,
                max_block=block_range.max,
            )

        except NotImplementedError:
            time_range = TimeRange.from_spec(range_spec)
            block_range = None
            chain_block_ranges = get_chain_block_ranges(chains, time_range)

        return cls(
            chains=chains,
            chain_block_ranges=chain_block_ranges,
            chain_max_blocks=get_chain_max_blocks(chains),
            time_range=time_range,
            root_paths_to_read=root_paths_to_read,
            block_range=block_range,
        )

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
        # Root paths in `markers_df` are all physical. This means that the
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
        location: DataLocation,
        markers_table: str | None = None,
        padded_dates: bool = False,
        extra_columns: list[str] | None = None,
    ) -> pl.DataFrame:
        """Query completion markers for a list of dates and chains.

        Returns a dataframe with the markers and all of the parquet output paths
        associated with them.
        """
        client = init_data_access()

        # Default values are for the blockbatch markers table.
        markers_table = markers_table or BLOCKBATCH_MARKERS_TABLE
        extra_columns = (
            extra_columns
            if extra_columns is not None
            else [
                "num_blocks",
                "min_block",
                "max_block",
            ]
        )

        if not padded_dates:
            datevals = self.time_range.to_date_range().dates()

        else:
            datevals = self.time_range.to_date_range().padded_dates()

        root_paths = self.physical_root_paths()
        log.info(f"querying markers at time range {self.time_range.logstr} {root_paths=}")
        return client.query_markers_by_root_path(
            chains=self.chains,
            datevals=datevals,
            data_location=location,
            root_paths=root_paths,
            markers_table=markers_table,
            extra_columns=extra_columns,
        )


@dataclass
class BlockBatchRequestData:
    # True if the data is complete and ready to be consumed.
    is_complete: bool

    # Physical parquet paths (values) that will be read for each logical root path (keys).
    data_paths: dict[str, list[str]] | None
