from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.rangeutils.daterange import DateRange
from op_analytics.coreutils.time import date_fromstr

from .dataaccess import DateFilter, MarkerFilter, init_data_access
from .dailydatawrite import MARKERS_TABLE
from .location import DataLocation

log = structlog.get_logger()


def query_parquet_paths(
    root_path: str,
    location: DataLocation,
    datefilter: DateFilter,
):
    partitioned_data_access = init_data_access()

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
    return paths


def make_date_filter(
    min_date: str | None = None, max_date: str | None = None, date_range_spec: str | None = None
) -> DateFilter:
    return DateFilter(
        min_date=None if min_date is None else date_fromstr(min_date),
        max_date=None if max_date is None else date_fromstr(max_date),
        datevals=None if date_range_spec is None else DateRange.from_spec(date_range_spec).dates(),
    )
