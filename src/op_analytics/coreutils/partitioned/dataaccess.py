"""Data Access Layer.

This module controls data access to markers and output parquet files.

The main goals are:

- Make data access easy to use in tests.
- Prevent accidental data access to real data from tests or local scripts.
"""

from datetime import date

import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.storage.gcs_parquet import gcs_upload_parquet, local_upload_parquet

from .location import DataLocation
from .markers_clickhouse import ClickHouseMarkers
from .markers_core import DateFilter, MarkerFilter, MarkerStore
from .markers_local import LocalMarkers

log = structlog.get_logger()

MARKERS_QUERY_SCHEMA = {
    "dt": pl.Date,
    "chain": pl.String,
    "marker_path": pl.String,
    "root_path": pl.String,
    "data_path": pl.String,
    "num_parts": pl.UInt32,
}


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
        markers_df = store.query_single_marker(
            marker_path=marker_path,
            markers_table=markers_table,
        )
        return check_marker(
            markers_df=markers_df,
            marker_path=marker_path,
        )

    def query_markers_with_filters(
        self,
        data_location: DataLocation,
        markers_table: str,
        datefilter: DateFilter,
        projections: list[str],
        filters=dict[str, MarkerFilter],
    ) -> pl.DataFrame:
        """Query completion markers with user provided filters and projections.

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

    def query_markers_by_root_path(
        self,
        chains: list[str],
        datevals: list[date],
        root_paths: list[str],
        data_location: DataLocation,
        markers_table: str,
        extra_columns: list[str],
    ) -> pl.DataFrame:
        """Query completion markers for a list of dates and chains.

        Returns a dataframe with the markers and all of the parquet output paths
        associated with them.
        """

        paths_df = self.query_markers_with_filters(
            data_location=data_location,
            markers_table=markers_table,
            datefilter=DateFilter(
                min_date=None,
                max_date=None,
                datevals=datevals,
            ),
            projections=[
                "dt",
                "chain",
                "marker_path",
                "data_path",
                "root_path",
                "num_parts",
            ]
            + extra_columns,
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

        default_columns_schema = {
            k: v for k, v in paths_df.schema.items() if k in MARKERS_QUERY_SCHEMA
        }
        assert default_columns_schema == MARKERS_QUERY_SCHEMA

        return paths_df


def check_marker(markers_df: pl.DataFrame | None, marker_path: str) -> bool:
    """Check if the marker is complete and correct."""
    if markers_df is None:
        return False

    df = markers_df.sql(f"""
        SELECT
            marker_path, num_parts, count(DISTINCT data_path) as num_paths
        FROM self
        WHERE marker_path = '{marker_path}'
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
        log.error(f"distinct data paths do not match expeted num parts for marker: {marker_path!r}")
        return False
