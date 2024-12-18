import polars as pl
import pyarrow as pa

from op_analytics.coreutils.duckdb_local.client import insert_duckdb_local, run_query_duckdb_local
from op_analytics.coreutils.env.aware import etl_monitor_markers_database
from op_analytics.coreutils.logger import structlog

from .markers_core import DateFilter, MarkerFilter

log = structlog.get_logger()


class LocalMarkers:
    _instance = None  # This class is a singleton

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @property
    def markers_db(self):
        return etl_monitor_markers_database()

    def write_marker(self, markers_table: str, marker_df: pa.Table):
        insert_duckdb_local(self.markers_db, markers_table, marker_df)

    def query_single_marker(self, marker_path: str, markers_table: str) -> pl.DataFrame:
        return run_query_duckdb_local(
            query=f"""
            SELECT
                marker_path, num_parts, data_path
            FROM {self.markers_db}.{markers_table} 
            WHERE marker_path = ?
            """,
            params=[marker_path],
        ).pl()

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
