import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dataaccess import init_data_access
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.coreutils.partitioned.markers_core import DateFilter, MarkerFilter
from op_analytics.coreutils.rangeutils.daterange import DateRange
from op_analytics.datapipeline.etl.ingestion.reader.request import BLOCKBATCH_MARKERS_TABLE

from .config import LOADABLE_ROOT_PATHS

log = structlog.get_logger()


# Table on Clickhouse that stores markers for data that has been loaded.
BLOCKBATCH_MARKERS_DW_TABLE = "blockbatch_markers_datawarehouse"

# Columns that we want to query from the markers table.
MARKER_COLUMNS = [
    "root_path",
    "chain",
    "dt",
    "min_block",
    "data_path",
]


def candidate_markers(date_range: DateRange) -> pl.DataFrame:
    """Query to find markers that exist in the daterange.

    Only markers from the configured ingestable root paths are considered.
    """

    markers_data_access = init_data_access()

    return markers_data_access.query_markers_with_filters(
        data_location=DataLocation.GCS,
        markers_table=BLOCKBATCH_MARKERS_TABLE,
        datefilter=DateFilter(
            min_date=date_range.min,
            max_date=date_range.max,
            datevals=None,
        ),
        projections=MARKER_COLUMNS,
        filters={
            "roots": MarkerFilter(column="root_path", values=LOADABLE_ROOT_PATHS),
        },
    )


def existing_markers(date_range: DateRange) -> pl.DataFrame:
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
        filters={},
    )
