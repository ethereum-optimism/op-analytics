"""Data Access Layer.

This module controls data access to markers and output parquet files.

The main goals are:

- Make data access easy to use in tests.
- Prevent accidental data access to real data from tests or local scripts.
"""

from dataclasses import dataclass
from datetime import date
from functools import cache
from threading import Lock
from typing import Any, Protocol

import polars as pl
import pyarrow as pa

from op_analytics.coreutils.duckdb_local.client import insert_duckdb_local, run_query_duckdb_local
from op_analytics.coreutils.clickhouse.oplabs import insert_oplabs, run_query_oplabs
from op_analytics.coreutils.env.aware import etl_monitor_markers_database
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.storage.gcs_parquet import (
    gcs_upload_parquet,
    local_upload_parquet,
)

from .location import DataLocation

log = structlog.get_logger()

_CLIENT = None

_INIT_LOCK = Lock()


def init_data_access() -> "PartitionedDataAccess":
    global _CLIENT

    with _INIT_LOCK:
        if _CLIENT is None:
            _CLIENT = PartitionedDataAccess()

    if _CLIENT is None:
        raise RuntimeError("Partitioned data access client was not properly initialized.")
    return _CLIENT


@dataclass
class MarkerFilter:
    column: str
    values: list[str]

    def clickhouse_filter(self, param: str):
        return " AND %s in {%s:Array(String)}" % (self.column, param)


@dataclass
class DateFilter:
    min_date: date | None
    max_date: date | None
    datevals: list[date] | None

    @property
    def is_undefined(self):
        return all(
            [
                self.min_date is None,
                self.max_date is None,
                self.datevals is None,
            ]
        )

    def sql_clickhouse(self):
        where = []
        parameters: dict[str, Any] = {}

        if self.datevals is not None and len(self.datevals) > 0:
            where.append("dt IN {dates:Array(Date)}")
            parameters["dates"] = self.datevals

        if self.min_date is not None:
            where.append("dt >= {mindate:Date}")
            parameters["mindate"] = self.min_date

        if self.max_date is not None:
            where.append("dt < {maxdate:Date}")
            parameters["maxdate"] = self.max_date

        return " AND ".join(where), parameters

    def sql_duckdb(self):
        where = []
        if self.datevals is not None and len(self.datevals) > 0:
            datelist = ", ".join([f"'{_.strftime("%Y-%m-%d")}'" for _ in self.datevals])
            where.append(f"dt IN ({datelist})")

        if self.min_date is not None:
            where.append(f"dt >= '{self.min_date.strftime("%Y-%m-%d")}'")

        if self.max_date is not None:
            where.append(f"dt < '{self.max_date.strftime("%Y-%m-%d")}'")

        return " AND ".join(where)


class MarkerStore(Protocol):
    def write_marker(
        self,
        markers_table: str,
        marker_df: pa.Table,
    ) -> None: ...

    def marker_exists(
        self,
        marker_path: str,
        markers_table: str,
    ) -> bool: ...

    def query_markers(
        self,
        markers_table: str,
        datefilter: DateFilter,
        projections=list[str],
        filters=dict[str, MarkerFilter],
    ) -> pl.DataFrame: ...


@cache
def marker_store(data_location: DataLocation) -> MarkerStore:
    """Store to use for a given DataLocation.

    - GCS markers go to ClickHouse
    - LOCAL markers to got local DuckDB
    """
    if data_location == DataLocation.GCS:
        return ClickHouseMarkers()

    if data_location == DataLocation.BIGQUERY:
        return ClickHouseMarkers()

    if data_location == DataLocation.LOCAL:
        return LocalMarkers()

    if data_location == DataLocation.BIGQUERY_LOCAL_MARKERS:
        return LocalMarkers()

    raise NotImplementedError(f"invalid data location: {data_location}")


@dataclass
class PartitionedDataAccess:
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
        """Run a query to find if a marker already exists."""
        store = marker_store(data_location)
        return store.marker_exists(
            marker_path=marker_path,
            markers_table=markers_table,
        )

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

        store = marker_store(data_location)
        return store.query_markers(
            markers_table=markers_table,
            datefilter=datefilter,
            projections=projections,
            filters=filters,
        )


def check_marker_results(df: pl.DataFrame) -> bool:
    """Check query results for a single marker.

    Datermien whether the marker is complete and correct."""
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


class ClickHouseMarkers:
    @property
    def markers_db(self):
        return etl_monitor_markers_database()

    def write_marker(self, markers_table: str, marker_df: pa.Table):
        insert_oplabs(self.markers_db, markers_table, marker_df)

    def marker_exists(self, marker_path: str, markers_table: str) -> bool:
        where = "marker_path = {search_value:String}"

        df = run_query_oplabs(
            query=f"""
            SELECT
                marker_path, num_parts, count(DISTINCT data_path) as num_paths
            FROM {self.markers_db}.{markers_table} 
            WHERE {where}
            GROUP BY marker_path, num_parts
            """,
            parameters={"search_value": marker_path},
        )
        return check_marker_results(df)

    def query_markers(
        self,
        markers_table: str,
        datefilter: DateFilter,
        projections=list[str],
        filters=dict[str, MarkerFilter],
    ) -> pl.DataFrame:
        """ClickHouse version of query many."""

        where, parameters = datefilter.sql_clickhouse()
        if not where:
            where = "1=1"

        for param, item in filters.items():
            where += item.clickhouse_filter(param)
            parameters[param] = item.values

        cols = ",\n".join(projections)

        markers = run_query_oplabs(
            query=f"""
            SELECT
                {cols}
            FROM {self.markers_db}.{markers_table}
            WHERE {where}
            """,
            parameters=parameters,
        )

        # ClickHouse returns the Date type as u16 days from epoch.
        return markers.with_columns(dt=pl.from_epoch(pl.col("dt"), time_unit="d"))


class LocalMarkers:
    @property
    def markers_db(self):
        return etl_monitor_markers_database()

    def write_marker(self, markers_table: str, marker_df: pa.Table):
        insert_duckdb_local(self.markers_db, markers_table, marker_df)

    def marker_exists(self, marker_path: str, markers_table: str) -> bool:
        df = run_query_duckdb_local(
            query=f"""
            SELECT
                marker_path, num_parts, count(DISTINCT data_path) as num_paths
            FROM {self.markers_db}.{markers_table} 
            WHERE marker_path = ?
            GROUP BY marker_path, num_parts
            """,
            params=[marker_path],
        ).pl()
        return check_marker_results(df)

    def query_markers(
        self,
        markers_table: str,
        datefilter: DateFilter,
        projections=list[str],
        filters=dict[str, MarkerFilter],
    ) -> pl.DataFrame:
        """DuckDB version of query many."""

        where = datefilter.sql_duckdb()
        if not where:
            where = "1=1"

        for _, item in filters.items():
            valueslist = ", ".join(f"'{_}'" for _ in item.values)
            where += f" AND {item.column} in ({valueslist})"

        cols = ",\n".join(projections)

        markers = run_query_duckdb_local(
            query=f"""
            SELECT
                {cols}
            FROM {self.markers_db}.{markers_table}
            WHERE {where}
            """,
        )

        return markers.pl()


def all_outputs_complete(
    location: DataLocation,
    markers: list[str],
    markers_table: str,
) -> bool:
    """Check if all outputs are complete.

    This function is somewhat low-level in that it receives the explicit completion
    markers that we are looking for. It checks that those markers are present in all
    of the data sinks.
    """
    client = init_data_access()

    complete = []
    incomplete = []

    # TODO: Make a single query for all the markers.
    for marker in markers:
        if client.marker_exists(location, marker, markers_table):
            complete.append(marker)
        else:
            incomplete.append(marker)

    num_complete = len(complete)
    total = len(incomplete) + len(complete)
    log.debug(f"{num_complete}/{total} complete")

    if incomplete:
        return False

    return True
