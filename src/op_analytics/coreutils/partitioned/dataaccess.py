"""Data Access Layer.

This module controls data access to markers and output parquet files.

The main goals are:

- Make data access easy to use in tests.
- Prevent accidental data access to real data from tests or local scripts.
"""

import polars as pl


from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.storage.gcs_parquet import gcs_upload_parquet, local_upload_parquet

from .location import DataLocation
from .markers_clickhouse import ClickHouseMarkers
from .markers_core import DateFilter, MarkerFilter, MarkerStore
from .markers_local import LocalMarkers

log = structlog.get_logger()


def init_data_access() -> "PartitionedDataAccess":
    return PartitionedDataAccess.get_instance()


def marker_store(data_location: DataLocation) -> MarkerStore:
    """Store to use for a given DataLocation.

    - GCS markers go to ClickHouse
    - LOCAL markers to got local DuckDB
    """
    if data_location == DataLocation.GCS:
        return ClickHouseMarkers.get_instance()

    if data_location == DataLocation.BIGQUERY:
        return ClickHouseMarkers.get_instance()

    if data_location == DataLocation.LOCAL:
        return LocalMarkers.get_instance()

    if data_location == DataLocation.BIGQUERY_LOCAL_MARKERS:
        return LocalMarkers.get_instance()

    raise NotImplementedError(f"invalid data location: {data_location}")


class PartitionedDataAccess:
    _instance = None  # This class is a singleton

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def write_single_part(
        self,
        location: DataLocation,
        dataframe: pl.DataFrame,
        full_path: str,
    ):
        """Write a single parquet output file for a partitioned output."""
        location.check_write_allowed()
        if location == DataLocation.GCS:
            gcs_upload_parquet(full_path, dataframe)
            return

        elif location == DataLocation.LOCAL:
            local_upload_parquet(
                path=location.with_prefix(full_path),
                df=dataframe,
            )

            return

        raise NotImplementedError()

    def write_marker(
        self,
        marker_df: pl.DataFrame,
        data_location: DataLocation,
        markers_table: str,
    ):
        """Write marker.

        Having markers allows us to quickly check completion and perform analytics
        over previous iterations of the ingestion process.

        Markers for GCS output are written to Clickhouse.
        Markers for local output are written to DuckDB

        """
        store = marker_store(data_location)
        store.write_marker(markers_table, marker_df)

    def marker_exists(
        self,
        data_location: DataLocation,
        marker_path: str,
        markers_table: str,
    ) -> bool:
        """Run a query to find if a marker already exists.

        Determine if the marker is complete and correct.
        """
        store: MarkerStore = marker_store(data_location)
        marker_df = store.query_single_marker(
            marker_path=marker_path,
            markers_table=markers_table,
        )

        df = marker_df.sql("""
            SELECT
                marker_path, num_parts, count(DISTINCT data_path) as num_paths
            FROM self
            GROUP BY marker_path, num_parts
            """)

        if len(df) == 0:
            return False

        assert len(df) == 1
        row = df.to_dicts()[0]

        if row["num_parts"] == row["num_paths"]:
            return True
        else:
            marker_path = row["marker_path"]
            log.error(
                f"distinct data paths do not match expeted num parts for marker: {marker_path!r}"
            )
            return False

    def all_markers_exist(
        self,
        data_location: DataLocation,
        marker_paths: list[str],
        markers_table: str,
    ) -> bool:
        complete = []
        incomplete = []

        # TODO: Make a single query for all the markers.
        for marker_path in marker_paths:
            if self.marker_exists(data_location, marker_path, markers_table):
                complete.append(marker_path)
            else:
                incomplete.append(marker_path)

        num_complete = len(complete)
        total = len(incomplete) + len(complete)
        log.debug(f"{num_complete}/{total} complete")

        if incomplete:
            return False

        return True

    def markers_for_dates(
        self,
        data_location: DataLocation,
        markers_table: str,
        datefilter: DateFilter,
        projections: list[str],
        filters=dict[str, MarkerFilter],
    ) -> pl.DataFrame:
        """Query completion markers.

        Returns a dataframe with the markers that match the provided filters
        including only the columns specified in projections.
        """
        if "dt" not in projections:
            raise ValueError("projections must include 'dt'")

        store: MarkerStore = marker_store(data_location)
        return store.query_markers(
            markers_table=markers_table,
            datefilter=datefilter,
            projections=projections,
            filters=filters,
        )


def complete_markers(
    location: DataLocation,
    markers: list[str],
    markers_table: str,
) -> list[str]:
    """List of markers that are complete.

    This function is somewhat low-level in that it receives the explicit completion
    markers that we are looking for. It checks that those markers are present in all
    of the data sinks.
    """
    client = init_data_access()

    complete = []

    # TODO: Make a single query for all the markers.
    for marker in markers:
        if client.marker_exists(location, marker, markers_table):
            complete.append(marker)

    num_complete = len(complete)
    log.debug(f"{num_complete}/{len(markers)} complete")

    return complete
