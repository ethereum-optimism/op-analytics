"""Data Access Layer.

This module controls data access to markers and output parquet files.

The main goals are:

- Make data access easy to use in tests.
- Prevent accidental data access to real data from tests or local scripts.
"""

from dataclasses import dataclass
from datetime import date
from threading import Lock
from typing import Any

import polars as pl

from op_analytics.coreutils import clickhouse, duckdb_local
from op_analytics.coreutils.env.aware import etl_monitor_markers_database
from op_analytics.coreutils.storage.gcs_parquet import (
    gcs_upload_parquet,
    local_upload_parquet,
)

from .location import DataLocation, MarkersLocation, marker_location
from .marker import Marker, OutputPartMeta
from .output import ExpectedOutput
from .types import PartitionedMarkerPath

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


@dataclass
class PartitionedDataAccess:
    @property
    def markers_db(self):
        return etl_monitor_markers_database()

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
        data_location: DataLocation,
        expected_output: ExpectedOutput,
        written_parts: list[OutputPartMeta],
        markers_table: str,
    ):
        """Write marker.

        Having markers allows us to quickly check completion and perform analytics
        over previous iterations of the ingestion process.

        Markers for GCS output are written to Clickhouse.
        Markers for local output are written to DuckDB

        """
        marker = Marker(
            expected_output=expected_output,
            written_parts=written_parts,
        )
        arrow_table = marker.to_pyarrow_table()

        if data_location in (DataLocation.GCS, DataLocation.BIGQUERY):
            clickhouse.insert_arrow("OPLABS", self.markers_db, markers_table, arrow_table)
            return

        elif data_location in (DataLocation.LOCAL, DataLocation.BIGQUERY_LOCAL_MARKERS):
            duckdb_local.insert_arrow(self.markers_db, markers_table, arrow_table)
            return

        raise NotImplementedError()

    def marker_exists(
        self,
        data_location: DataLocation,
        marker_path: PartitionedMarkerPath,
        markers_table: str,
    ) -> bool:
        """Run a query to find if a marker already exists."""
        store = marker_location(data_location)

        if store == MarkersLocation.OPLABS_CLICKHOUSE:
            result = self._query_one_clickhouse(marker_path, markers_table)
        else:
            # default to DUCKDB_LOCAL
            result = self._query_one_duckdb(marker_path, markers_table)

        return len(result) > 0

    def markers_for_raw_ingestion(
        self,
        data_location: DataLocation,
        markers_table: str,
        datevals: list[date],
        chains: list[str],
        root_paths: list[str],
    ) -> pl.DataFrame:
        """Query completion markers for a list of dates and chains.

        Returns a dataframe with the markers and all of the parquet output paths
        associated with them.
        """
        paths_df = self.markers_for_dates(
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
                "num_blocks",
                "min_block",
                "max_block",
                "data_path",
                "root_path",
            ],
            filters={
                "chains": MarkerFilter(
                    column="chain",
                    values=chains,
                ),
                "datasets": MarkerFilter(
                    column="root_path",
                    values=root_paths,
                ),
            },
        )

        assert dict(paths_df.schema) == {
            "dt": pl.Date,
            "chain": pl.String,
            "num_blocks": pl.Int32,
            "min_block": pl.Int64,
            "max_block": pl.Int64,
            "root_path": pl.String,
            "data_path": pl.String,
        }

        return paths_df

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
        store = marker_location(data_location)

        if "dt" not in projections:
            raise ValueError("projections must include 'dt'")

        if store == MarkersLocation.OPLABS_CLICKHOUSE:
            paths_df = self._query_many_clickhouse(
                markers_table=markers_table,
                datefilter=datefilter,
                projections=projections,
                filters=filters,
            )
        else:
            # default to DUCKDB_LOCAL
            paths_df = self._query_many_duckdb(
                markers_table=markers_table,
                datefilter=datefilter,
                projections=projections,
                filters=filters,
            )

        return paths_df

    def _query_one_clickhouse(self, marker_path: PartitionedMarkerPath, markers_table: str):
        where = "marker_path = {search_value:String}"

        return clickhouse.run_oplabs_query(
            query=f"SELECT marker_path FROM {self.markers_db}.{markers_table} WHERE {where}",
            parameters={"search_value": marker_path},
        )

    def _query_one_duckdb(self, marker_path: PartitionedMarkerPath, markers_table: str):
        return duckdb_local.run_query(
            query=f"SELECT marker_path FROM {self.markers_db}.{markers_table} WHERE marker_path = ?",
            params=[marker_path],
        )

    def _query_many_clickhouse(
        self,
        markers_table: str,
        datefilter: DateFilter,
        projections=list[str],
        filters=dict[str, MarkerFilter],
    ):
        """ClickHouse version of query many."""

        where, parameters = datefilter.sql_clickhouse()
        if not where:
            where = "1=1"

        for param, item in filters.items():
            where += item.clickhouse_filter(param)
            parameters[param] = item.values

        cols = ",\n".join(projections)

        markers = clickhouse.run_oplabs_query(
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

    def _query_many_duckdb(
        self,
        markers_table: str,
        datefilter: DateFilter,
        projections=list[str],
        filters=dict[str, MarkerFilter],
    ):
        """DuckDB version of query many."""

        where = datefilter.sql_duckdb()
        if not where:
            where = "1=1"

        for _, item in filters.items():
            valueslist = ", ".join(f"'{_}'" for _ in item.values)
            where += f" AND {item.column} in ({valueslist})"

        cols = ",\n".join(projections)

        markers = duckdb_local.run_query(
            query=f"""
            SELECT
                {cols}
            FROM {self.markers_db}.{markers_table}
            WHERE {where}
            """,
        )

        return markers.pl()
