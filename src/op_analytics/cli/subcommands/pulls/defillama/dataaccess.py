import glob
import os
from enum import Enum
from functools import cache

import polars as pl
import pyarrow as pa

from op_analytics.coreutils.duckdb_inmem import init_client
from op_analytics.coreutils.env.aware import OPLabsEnvironment, current_environment
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned import (
    DataLocation,
    DataWriter,
    ExpectedOutput,
    OutputData,
    PartitionedMarkerPath,
    PartitionedRootPath,
)
from op_analytics.coreutils.partitioned.breakout import breakout_partitions
from op_analytics.coreutils.partitioned.dataaccess import DateFilter, MarkerFilter, init_data_access
from op_analytics.coreutils.path import repo_path
from op_analytics.coreutils.time import date_fromstr, now_friendly_timestamp
from op_analytics.datapipeline.utils.daterange import DateRange

log = structlog.get_logger()


MARKERS_TABLE = "daily_data_markers"


class DefiLlama(str, Enum):
    """Supported defillama datasets.

    This class includes utilities to read data from each dataset from a notebook
    for ad-hoc use cases.
    """

    CHAINS_METADATA = "chains_metadata_v1"

    HISTORICAL_CHAIN_TVL = "historical_chain_tvl_v1"

    PROTOCOLS_METADATA = "protocols_metadata_v1"
    PROTOCOLS_TVL = "protocols_tvl_v1"
    PROTOCOLS_TOKEN_TVL = "protocols_token_tvl_v1"

    def read(
        self,
        min_date: str | None = None,
        max_date: str | None = None,
        date_range_spec: str | None = None,
    ):
        """Read defillama data. Optionally filtered by date."""
        return load(
            dataset_name=self.value,
            datefilter=self._date_filter(min_date, max_date, date_range_spec),
            location=DataLocation.GCS,
        )

    def dump_local_copy(
        self,
        min_date: str | None = None,
        max_date: str | None = None,
        date_range_spec: str | None = None,
    ):
        """Dump a copy of the data to a single local parquet file."""
        client = self.read(min_date, max_date, date_range_spec)

        path = self._local_path()
        log.info(f"begin dump copy to {path!r}")
        client.view(self.value).write_parquet(path, compression="zstd")
        log.info(f"done dump copy to {path!r}")
        self._register_local_copy(path)

    def load_local_copy(self, timestamp: str | None = None):
        """Load a local parquet file.

        If timestamp is not provided the last created local copy will be loaded.
        """

        dirname = os.path.dirname(self._local_path())
        if timestamp is None:
            candidates = sorted(
                glob.glob(os.path.join(dirname, f"{self.value}__*.parquet")), reverse=True
            )
            if not candidates:
                raise Exception(f"no local copies found for {self.value!r}")
            path = candidates[0]
        else:
            path = os.path.join(dirname, f"{self.value}__{timestamp}.parquet")

        log.info(f"load copy from {path!r}")
        return self._register_local_copy(path)

    def _register_local_copy(self, path: str):
        client = init_client()

        client.register(
            view_name=os.path.basename(path).removesuffix(".parquet"),
            python_object=client.read_parquet(path),
        )

        print(client.sql("SHOW TABLES"))
        return client

    @staticmethod
    def _date_filter(
        min_date: str | None = None, max_date: str | None = None, date_range_spec: str | None = None
    ) -> DateFilter:
        return DateFilter(
            min_date=None if min_date is None else date_fromstr(min_date),
            max_date=None if max_date is None else date_fromstr(max_date),
            datevals=None
            if date_range_spec is None
            else DateRange.from_spec(date_range_spec).dates,
        )

    def _local_path(self):
        path = repo_path(f"parquet/defillama/{self.value}__{now_friendly_timestamp()}.parquet")
        assert path is not None
        os.makedirs(os.path.dirname(path), exist_ok=True)
        return path


@cache
def write_location():
    if current_environment() == OPLabsEnvironment.UNITTEST:
        return DataLocation.LOCAL
    else:
        return DataLocation.GCS


def write(
    dataset: DefiLlama,
    dataframe: pl.DataFrame,
    sort_by: list[str] | None = None,
    force_complete: bool = False,
):
    """Write date partitioned defillama dataset."""
    parts = breakout_partitions(
        df=dataframe,
        partition_cols=["dt"],
        default_partition=None,
    )

    for part in parts:
        root = f"defillama/{dataset.value}"
        datestr = part.partition_value("dt")

        writer = DataWriter(
            write_to=write_location(),
            partition_cols=["dt"],
            markers_table=MARKERS_TABLE,
            expected_outputs=[
                ExpectedOutput(
                    dataset_name=dataset.value,
                    root_path=PartitionedRootPath(root),
                    file_name="out.parquet",
                    marker_path=PartitionedMarkerPath(f"{datestr}/{root}"),
                    process_name="default",
                    additional_columns=dict(),
                    additional_columns_schema=[
                        pa.field("dt", pa.date32()),
                    ],
                )
            ],
            force=force_complete,
        )

        part_df = part.df.with_columns(dt=pl.lit(datestr))

        if sort_by is not None:
            part_df = part_df.sort(*sort_by)

        writer.write(
            output_data=OutputData(
                dataframe=part.df.with_columns(dt=pl.lit(datestr)),
                dataset_name=dataset.value,
                default_partition=None,
            )
        )


def load(
    dataset_name: str,
    datefilter: DateFilter,
    location: DataLocation = DataLocation.GCS,
):
    """Load date partitioned defillama dataset from the specified location.

    The loaded data is registered as duckdb view.
    """
    partitioned_data_access = init_data_access()
    duckdb_client = init_client()

    paths: str | list[str]
    if datefilter.is_undefined:
        paths = location.absolute(f"defillama/{dataset_name}/dt=*/out.parquet")
        summary = f"using uri wildcard {paths!r}"

    else:
        log.info(f"querying markers for {dataset_name!r} {datefilter}")

        markers = partitioned_data_access.markers_for_dates(
            data_location=location,
            markers_table=MARKERS_TABLE,
            datefilter=datefilter,
            projections=["dt", "data_path"],
            filters={
                "datasets": MarkerFilter(
                    column="dataset_name",
                    values=[dataset_name],
                ),
            },
        )
        log.info(f"{len(markers)} markers found")

        paths = [location.absolute(path) for path in markers["data_path"].to_list()]
        summary = f"using {len(paths)} parquet paths"

    duckdb_client.register(
        view_name=dataset_name,
        python_object=duckdb_client.read_parquet(
            paths,
            hive_partitioning=True,
        ),
    )
    log.info(f"registered view {dataset_name!r} {summary}")

    print(duckdb_client.sql("SHOW TABLES"))
    return duckdb_client
