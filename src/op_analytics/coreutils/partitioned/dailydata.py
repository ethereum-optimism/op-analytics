from datetime import timedelta
from enum import Enum
from functools import cache

import polars as pl
import pyarrow as pa

from op_analytics.coreutils.clickhouse.oplabs import (
    insert_oplabs,
    run_query_oplabs,
)
from op_analytics.coreutils.duckdb_inmem import EmptyParquetData
from op_analytics.coreutils.duckdb_inmem.client import init_client, register_parquet_relation
from op_analytics.coreutils.env.aware import is_bot
from op_analytics.coreutils.logger import structlog, human_rows, human_size
from op_analytics.coreutils.rangeutils.daterange import DateRange
from op_analytics.coreutils.time import date_fromstr

from .breakout import breakout_partitions
from .dataaccess import DateFilter, MarkerFilter, init_data_access
from .location import DataLocation
from .output import ExpectedOutput, OutputData
from .writer import PartitionedWriteManager

log = structlog.get_logger()


MARKERS_TABLE = "daily_data_markers"


@cache
def determine_location() -> DataLocation:
    # Only for github actions or k8s we use GCS.
    if is_bot():
        return DataLocation.GCS

    # For unittests and local runs we use LOCAL.
    return DataLocation.LOCAL


def write_daily_data(
    root_path: str,
    dataframe: pl.DataFrame,
    sort_by: list[str] | None = None,
):
    """Write date partitioned defillama dataset.

    NOTE: This method always overwrites data. If we had already pulled in data for
    a given date a subsequent data pull will always overwrite it.
    """

    parts = breakout_partitions(
        df=dataframe,
        partition_cols=["dt"],
        default_partitions=None,
    )

    # Ensure write location for tests is LOCAL.
    location = determine_location()

    for part in parts:
        datestr = part.partition_value("dt")

        writer = PartitionedWriteManager(
            process_name="default",
            location=location,
            partition_cols=["dt"],
            extra_marker_columns=dict(),
            extra_marker_columns_schema=[
                pa.field("dt", pa.date32()),
            ],
            markers_table=MARKERS_TABLE,
            expected_outputs=[
                ExpectedOutput(
                    root_path=root_path,
                    file_name="out.parquet",
                    marker_path=f"{datestr}/{root_path}",
                )
            ],
        )

        part_df = part.df.with_columns(dt=pl.lit(datestr))

        if sort_by is not None:
            part_df = part_df.sort(*sort_by)

        writer.write(
            output_data=OutputData(
                dataframe=part.df.with_columns(dt=pl.lit(datestr)),
                root_path=root_path,
                default_partitions=None,
            )
        )


def read_daily_data(
    root_path: str,
    min_date: str | None = None,
    max_date: str | None = None,
    date_range_spec: str | None = None,
    location: DataLocation = DataLocation.GCS,
) -> str:
    """Load date partitioned defillama dataset from the specified location.

    The loaded data is registered as duckdb view.

    The name of the registered view is returned.
    """
    partitioned_data_access = init_data_access()
    duckdb_context = init_client()

    datefilter = make_date_filter(min_date, max_date, date_range_spec)

    paths: str | list[str]
    if datefilter.is_undefined:
        paths = location.absolute(f"{root_path}/dt=*/out.parquet")

    else:
        log.info(f"querying markers for {root_path!r} {datefilter}")

        markers = partitioned_data_access.query_markers_with_filters(
            data_location=location,
            markers_table=MARKERS_TABLE,
            datefilter=datefilter,
            projections=["dt", "data_path"],
            filters={
                "root_paths": MarkerFilter(
                    column="root_path",
                    values=[root_path],
                ),
            },
        )
        log.info(
            f"{len(markers)} markers found",
            min_dt=str(markers["dt"].min()),
            max_dt=str(markers["dt"].max()),
        )

        # Ensure that the paths we select are distinct paths.
        # The same path can appear under two different markers if it was
        # re-written as part of a backfill.
        paths = sorted(set([location.absolute(path) for path in markers["data_path"].to_list()]))
        log.info(f"{len(set(paths))} distinct paths")

    view_name = register_parquet_relation(dataset=root_path, parquet_paths=paths)
    print(duckdb_context.client.sql("SHOW TABLES"))
    return view_name


def make_date_filter(
    min_date: str | None = None, max_date: str | None = None, date_range_spec: str | None = None
) -> DateFilter:
    return DateFilter(
        min_date=None if min_date is None else date_fromstr(min_date),
        max_date=None if max_date is None else date_fromstr(max_date),
        datevals=None if date_range_spec is None else DateRange.from_spec(date_range_spec).dates(),
    )


class DailyDataset(str, Enum):
    """Base class for daily datasets.

    The name of the subclass is the name of the dataset and the enum values
    are names of the tables that are part of the dataset.

    See for example: DefiLlama, GrowThePie
    """

    @property
    def db(self):
        return self.__class__.__name__.lower()

    @property
    def table(self):
        return self.value

    @property
    def root_path(self):
        return f"{self.db}/{self.table}"

    def read(
        self,
        min_date: str | None = None,
        max_date: str | None = None,
        date_range_spec: str | None = None,
    ) -> str:
        raise NotImplementedError()


def insert_daily_data_to_clickhouse(
    dataset: DailyDataset,
    min_date: str | None = None,
    max_date: str | None = None,
):
    """Incrementally load daily data from GCS to clickhouse.

    When date ranges are not provided this function queries clickhouse to find the latest loaded
    date and proceeds from there.

    When a backfill is needed you can provide min_date/max_date accordingly.
    """
    db = dataset.db
    table = dataset.table

    used_clickhouse_watermark = False
    if min_date is None:
        clickhouse_watermark = (
            run_query_oplabs(f"SELECT max(dt) as max_dt FROM {db}.{table}")
            .with_columns(max_dt=pl.from_epoch(pl.col("max_dt"), time_unit="d"))
            .item()
        )
        min_date = (clickhouse_watermark + timedelta(days=1)).strftime("%Y-%m-%d")
        used_clickhouse_watermark = True

    log.info(
        "incremental load to clickhouse",
        min_date=min_date,
        max_date=max_date,
        used_clickhouse_watermark=used_clickhouse_watermark,
    )

    try:
        fundamentals = dataset.read(min_date=min_date, max_date=max_date)
    except EmptyParquetData:
        log.info("incremental load: did not find new data")
        return

    duckdb_ctx = init_client()
    rel = duckdb_ctx.client.sql(f"SELECT * FROM {fundamentals}")
    query_summary = insert_oplabs(database=db, table=dataset.value, df_arrow=rel.arrow())

    log.info(
        "insert summary",
        written_bytes=human_size(query_summary.written_bytes()),
        written_rows=human_rows(query_summary.written_rows),
    )
