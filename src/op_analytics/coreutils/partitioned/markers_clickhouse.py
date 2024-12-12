import polars as pl
import pyarrow as pa

from op_analytics.coreutils.clickhouse.oplabs import insert_oplabs, run_query_oplabs
from op_analytics.coreutils.env.aware import etl_monitor_markers_database
from op_analytics.coreutils.logger import structlog

from .markers_core import DateFilter, MarkerFilter

log = structlog.get_logger()


class ClickHouseMarkers:
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
        insert_oplabs(self.markers_db, markers_table, marker_df)

    def query_single_marker(self, marker_path: str, markers_table: str) -> pl.DataFrame:
        where = "marker_path = {search_value:String}"

        return run_query_oplabs(
            query=f"""
            SELECT
                marker_path, num_parts, data_path
            FROM {self.markers_db}.{markers_table} 
            WHERE {where}
            """,
            parameters={"search_value": marker_path},
        )

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
