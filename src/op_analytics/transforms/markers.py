import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dataaccess import init_data_access
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.coreutils.partitioned.markers_core import DateFilter, MarkerFilter
from op_analytics.coreutils.rangeutils.daterange import DateRange


log = structlog.get_logger()


# Table on Clickhouse that stores markers for data that has been transformed.
TRANSFORM_MARKERS_TABLE = "transform_dt_markers"

# Columns that we want to query from the markers table.
MARKER_COLUMNS = [
    "transform",
    "dt",
]


def existing_markers(transforms: list[str], date_range: DateRange) -> pl.DataFrame:
    """Query to find markers for dt values that have already been processed."""

    markers_data_access = init_data_access()

    return markers_data_access.query_markers_with_filters(
        data_location=DataLocation.GCS,
        markers_table=TRANSFORM_MARKERS_TABLE,
        datefilter=DateFilter(
            min_date=date_range.min,
            max_date=date_range.max,
            datevals=None,
        ),
        projections=MARKER_COLUMNS,
        filters={
            "transforms": MarkerFilter(column="transform", values=transforms),
        },
    )
