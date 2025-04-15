import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dataaccess import init_data_access
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.coreutils.partitioned.markers_core import DateFilter, MarkerFilter
from op_analytics.coreutils.rangeutils.daterange import DateRange

log = structlog.get_logger()


# Table on Clickhouse that stores markers for data that has been loaded.
BLOCKBATCH_MARKERS_DW_TABLE = "blockbatch_daily"

# Columns that we want to query from the markers table.
MARKER_COLUMNS = [
    "root_path",
    "chain",
    "dt",
]


def query_blockbatch_daily_markers(
    date_range: DateRange,
    chains: list[str],
    root_paths: list[str],
) -> pl.DataFrame:
    """Query to find markers that have already been ingested in the date range."""

    markers_data_access = init_data_access()

    return markers_data_access.query_markers_with_filters(
        data_location=DataLocation.GCS,
        markers_table=BLOCKBATCH_MARKERS_DW_TABLE,
        datefilter=DateFilter(
            min_date=date_range.min,
            max_date=date_range.max,
            datevals=None,
        ),
        projections=MARKER_COLUMNS,
        filters={
            "chains": MarkerFilter(
                column="chain",
                values=chains,
            ),
            "root_paths": MarkerFilter(
                column="root_path",
                values=root_paths,
            ),
        },
    )
