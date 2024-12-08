import polars as pl
import pyarrow as pa
from functools import cache

from op_analytics.coreutils.env.aware import OPLabsEnvironment, current_environment
from op_analytics.coreutils.duckdb_inmem.client import init_client, register_parquet_relation
from op_analytics.coreutils.logger import structlog
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
def write_location():
    if current_environment() == OPLabsEnvironment.UNITTEST:
        return DataLocation.LOCAL
    else:
        return DataLocation.GCS


def write_daily_data(
    root_path: str,
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
        datestr = part.partition_value("dt")

        writer = PartitionedWriteManager(
            location=write_location(),
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
                    process_name="default",
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
                root_path=root_path,
                default_partition=None,
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
    duckdb_client = init_client()

    datefilter = make_date_filter(min_date, max_date, date_range_spec)

    paths: str | list[str]
    if datefilter.is_undefined:
        paths = location.absolute(f"{root_path}/dt=*/out.parquet")

    else:
        log.info(f"querying markers for {root_path!r} {datefilter}")

        markers = partitioned_data_access.markers_for_dates(
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
        log.info(f"{len(markers)} markers found")

        # Ensure that the paths we select are distinct paths.
        # The same path can appear under two different markers if it was
        # re-written as part of a backfill.
        paths = sorted(set([location.absolute(path) for path in markers["data_path"].to_list()]))
        log.info(f"{len(set(paths))} distinct paths")

    view_name = register_parquet_relation(dataset=root_path, parquet_paths=paths)
    print(duckdb_client.sql("SHOW TABLES"))
    return view_name


def make_date_filter(
    min_date: str | None = None, max_date: str | None = None, date_range_spec: str | None = None
) -> DateFilter:
    return DateFilter(
        min_date=None if min_date is None else date_fromstr(min_date),
        max_date=None if max_date is None else date_fromstr(max_date),
        datevals=None if date_range_spec is None else DateRange.from_spec(date_range_spec).dates(),
    )
