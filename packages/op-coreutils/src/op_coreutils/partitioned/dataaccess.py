"""Data Access Layer.

This module controls data access to markers and output parquet files.

The main goals are:

- Make data access easy to use in tests.
- Prevent accidental data access to real data from tests or local scripts.
"""

from datetime import date

import polars as pl

from op_coreutils import clickhouse, duckdb_local

from .location import DataLocation, MarkersLocation, marker_location
from .types import SinkMarkerPath


MARKERS_DB = "etl_monitor"


def init_data_access():
    return Access()


class Access:
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

    def markers_for_dates(
        self,
        data_location: DataLocation,
        datevals: list[date],
        chains: list[str],
        markers_table: str,
        dataset_names: list[str],
    ) -> pl.DataFrame:
        """Query completion markers for a list of dates and chains.

        Returns a dataframe with the markers and all of the parquet output paths
        associated with them.
        """
        store = marker_location(data_location)

        if store == MarkersLocation.OPLABS_CLICKHOUSE:
            paths_df = self._query_many_clickhouse(datevals, chains, markers_table, dataset_names)
        else:
            # default to DUCKDB_LOCAL
            paths_df = self._query_many_duckdb(datevals, chains, markers_table, dataset_names)

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

    def _query_one_clickhouse(self, marker_path: SinkMarkerPath, markers_table: str):
        where = "marker_path = {search_value:String}"

        return clickhouse.run_oplabs_query(
            query=f"SELECT marker_path FROM {MARKERS_DB}.{markers_table} WHERE {where}",
            parameters={"search_value": marker_path},
        )

    def _query_one_duckdb(self, marker_path: SinkMarkerPath, markers_table: str):
        return duckdb_local.run_query(
            query=f"SELECT marker_path FROM {MARKERS_DB}.{markers_table} WHERE marker_path = ?",
            params=[marker_path],
        )

    def _query_many_clickhouse(
        self,
        datevals: list[date],
        chains: list[str],
        markers_table: str,
        dataset_names: list[str],
    ):
        """ClickHouse version of query many."""

        where = "dt IN {dates:Array(Date)} AND chain in {chains:Array(String)} AND dataset_name in {datasets:Array(String)}"

        markers = clickhouse.run_oplabs_query(
            query=f"""
            SELECT
                dt,
                chain,
                num_blocks,
                min_block,
                max_block,
                data_path,
                dataset_name
            FROM {MARKERS_DB}.{markers_table}
            WHERE {where}
            """,
            parameters={
                "dates": datevals,
                "chains": chains,
                "datasets": dataset_names,
            },
        )

        # ClickHouse returns the Date type as u16 days from epoch.
        return markers.with_columns(dt=pl.from_epoch(pl.col("dt"), time_unit="d"))

    def _query_many_duckdb(
        self,
        datevals: list[date],
        chains: list[str],
        markers_table: str,
        dataset_names: list[str],
    ):
        """DuckDB version of query many."""

        datelist = ", ".join([f"'{_.strftime("%Y-%m-%d")}'" for _ in datevals])
        chainlist = ", ".join(f"'{_}'" for _ in chains)
        datasetlist = ", ".join(f"'{_}'" for _ in dataset_names)

        markers = duckdb_local.run_query(
            query=f"""
            SELECT
                dt,
                chain,
                num_blocks,
                min_block,
                max_block,
                data_path,
                dataset_name
            FROM {MARKERS_DB}.{markers_table}
            WHERE dt IN ({datelist}) AND chain in ({chainlist}) AND dataset_name in ({datasetlist})
            """,
        )

        return markers.pl()
