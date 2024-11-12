"""Data Access Layer.

This module controls data access to markers and output parquet files.

The main goals are:

- Make data access easy to use in tests.
- Prevent accidental data access to real data from tests or local scripts.
"""

from dataclasses import dataclass
from datetime import date
from threading import Lock

import polars as pl

from op_coreutils import clickhouse, duckdb_local
from op_coreutils.env.aware import OPLabsEnvironment, current_environment
from op_coreutils.path import repo_path
from op_coreutils.storage.gcs_parquet import (
    gcs_upload_parquet,
    local_upload_parquet,
)

from .location import DataLocation, MarkersLocation, marker_location
from .marker import Marker
from .output import ExpectedOutput, OutputPartMeta
from .types import SinkMarkerPath

_CLIENT = None

_INIT_LOCK = Lock()


def init_data_access() -> "PartitionedDataAccess":
    global _CLIENT

    with _INIT_LOCK:
        if _CLIENT is None:
            current_env = current_environment()
            if current_env == OPLabsEnvironment.UNITTEST:
                markers_db = "etl_monitor_dev"
            else:
                markers_db = "etl_monitor"

            # Create the schemas we need.
            duckdb_local.run_query(f"CREATE SCHEMA IF NOT EXISTS {markers_db}")

            # Create the tables we need.
            for database, table in [
                ("etl_monitor", "raw_onchain_ingestion_markers"),
                ("etl_monitor", "intermediate_model_markers"),
                ("etl_monitor", "superchain_raw_bigquery_markers"),
            ]:
                with open(repo_path(f"ddl/duckdb_local/{database}.{table}.sql"), "r") as fobj:
                    query = fobj.read().replace(database, markers_db)
                    duckdb_local.run_query(query)

            _CLIENT = PartitionedDataAccess(markers_db=markers_db)

    if _CLIENT is None:
        raise RuntimeError("Partitioned data access client was not properly initialized.")
    return _CLIENT


@dataclass
class MarkerFilter:
    column: str
    values: list[str]

    def clickhouse_filter(self, param: str):
        return "AND %s in {%s:Array(String)}" % (self.column, param)


@dataclass
class PartitionedDataAccess:
    markers_db: str

    def write_single_part(
        self,
        location: DataLocation,
        dataframe: pl.DataFrame,
        full_path: str,
    ):
        """Write a single parquet output file for a partitioned output."""
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
        marker_path: SinkMarkerPath,
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
        dataset_names: list[str],
    ) -> pl.DataFrame:
        paths_df = self.markers_for_dates(
            data_location=data_location,
            markers_table=markers_table,
            datevals=datevals,
            projections=[
                "dt",
                "chain",
                "num_blocks",
                "min_block",
                "max_block",
                "data_path",
                "dataset_name",
            ],
            filters={
                "chains": MarkerFilter(
                    column="chain",
                    values=chains,
                ),
                "datasets": MarkerFilter(
                    column="dataset_name",
                    values=dataset_names,
                ),
            },
        )

        assert paths_df.schema == {
            "dt": pl.Date,
            "chain": pl.String,
            "num_blocks": pl.Int32,
            "min_block": pl.Int64,
            "max_block": pl.Int64,
            "dataset_name": pl.String,
            "data_path": pl.String,
        }

        return paths_df

    def markers_for_dates(
        self,
        data_location: DataLocation,
        markers_table: str,
        datevals: list[date],
        projections: list[str],
        filters=dict[str, MarkerFilter],
    ) -> pl.DataFrame:
        """Query completion markers for a list of dates and chains.

        Returns a dataframe with the markers and all of the parquet output paths
        associated with them.
        """
        store = marker_location(data_location)

        if store == MarkersLocation.OPLABS_CLICKHOUSE:
            paths_df = self._query_many_clickhouse(
                markers_table=markers_table,
                datevals=datevals,
                projections=projections,
                filters=filters,
            )
        else:
            # default to DUCKDB_LOCAL
            paths_df = self._query_many_duckdb(
                markers_table=markers_table,
                datevals=datevals,
                projections=projections,
                filters=filters,
            )

        return paths_df

    def _query_one_clickhouse(self, marker_path: SinkMarkerPath, markers_table: str):
        where = "marker_path = {search_value:String}"

        return clickhouse.run_oplabs_query(
            query=f"SELECT marker_path FROM {self.markers_db}.{markers_table} WHERE {where}",
            parameters={"search_value": marker_path},
        )

    def _query_one_duckdb(self, marker_path: SinkMarkerPath, markers_table: str):
        return duckdb_local.run_query(
            query=f"SELECT marker_path FROM {self.markers_db}.{markers_table} WHERE marker_path = ?",
            params=[marker_path],
        )

    def _query_many_clickhouse(
        self,
        markers_table: str,
        datevals: list[date],
        projections=list[str],
        filters=dict[str, MarkerFilter],
    ):
        """ClickHouse version of query many."""

        where = "dt IN {dates:Array(Date)}"
        parameters = {"dates": datevals}

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
        datevals: list[date],
        projections=list[str],
        filters=dict[str, MarkerFilter],
    ):
        """DuckDB version of query many."""

        datelist = ", ".join([f"'{_.strftime("%Y-%m-%d")}'" for _ in datevals])
        where = f"dt IN ({datelist})"

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
