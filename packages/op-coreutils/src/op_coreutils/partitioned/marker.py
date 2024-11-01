import socket
from dataclasses import dataclass
from datetime import date
from typing import Any

import polars as pl
import pyarrow as pa

from op_coreutils import clickhouse, duckdb_local
from op_coreutils.time import date_fromstr, now

from .location import DataLocation, MarkersLocation, marker_location
from .output import WrittenParquetPath
from .types import SinkMarkerPath, SinkOutputRootPath


@dataclass
class Marker:
    """Represent a marker for a collection of objects written to storage."""

    marker_path: SinkMarkerPath
    dataset_name: str
    root_path: SinkOutputRootPath
    data_paths: list[WrittenParquetPath]
    process_name: str

    # Values for additional columns stored in the markers table.
    additional_columns: dict[str, Any]
    additional_columns_schema: list[pa.Field]

    def arrow_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("updated_at", pa.timestamp(unit="us", tz=None)),
                pa.field("marker_path", pa.string()),
                pa.field("dataset_name", pa.string()),
                pa.field("root_path", pa.string()),
                pa.field("num_parts", pa.int32()),
                pa.field("data_path", pa.string()),
                pa.field("row_count", pa.int64()),
                pa.field("process_name", pa.string()),
                pa.field("writer_name", pa.string()),
            ]
            + self.additional_columns_schema
        )

    def to_pyarrow_table(self) -> pa.Table:
        schema = self.arrow_schema()

        current_time = now()
        hostname = socket.gethostname()
        rows = []
        for parquet_out in self.data_paths:
            parquet_out_row = {
                "updated_at": current_time,
                "marker_path": self.marker_path,
                "root_path": self.root_path,
                "num_parts": len(self.data_paths),
                "dataset_name": self.dataset_name,
                "data_path": parquet_out.full_path,
                "row_count": parquet_out.row_count,
                "process_name": self.process_name,
                "writer_name": hostname,
            }

            for partition in parquet_out.partitions:
                sch = schema.field(partition.key)
                if sch.type == pa.date32():
                    parquet_out_row[partition.key] = date_fromstr(partition.value)
                else:
                    parquet_out_row[partition.key] = partition.value

            rows.append(parquet_out_row)

        for row in rows:
            for name, value in self.additional_columns.items():
                row[name] = value

        return pa.Table.from_pylist(rows, schema=schema)


MARKERS_DB = "etl_monitor"
MARKERS_TABLE = "raw_onchain_ingestion_markers"


def marker_exists(
    data_location: DataLocation,
    marker_path: SinkMarkerPath,
) -> bool:
    """Run a query to find if a marker already exists."""
    store = marker_location(data_location)

    if store == MarkersLocation.OPLABS_CLICKHOUSE:
        result = _query_one_clickhouse(marker_path)
    else:
        # default to DUCKDB_LOCAL
        result = _query_one_duckdb(marker_path)

    return len(result) > 0


def markers_for_dates(
    data_location: DataLocation,
    datevals: list[date],
    chains: list[str],
) -> pl.DataFrame:
    """Query completion markers for a list of dates and chains.

    Returns a dataframe with the markers and all of the parquet output paths
    associated with them.


    IMPORANT TODO: This function must accept a table parameter so we can query
    markers stored in different tables. It should also accept a dataset_names
    parameters so that we can target markers for a specific dataset.
    """
    store = marker_location(data_location)

    if store == MarkersLocation.OPLABS_CLICKHOUSE:
        paths_df = _query_many_clickhouse(datevals, chains)
    else:
        # default to DUCKDB_LOCAL
        paths_df = _query_many_duckdb(datevals, chains)

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


def _query_one_clickhouse(marker_path: SinkMarkerPath):
    where = "marker_path = {search_value:String}"

    return clickhouse.run_oplabs_query(
        query=f"SELECT marker_path FROM {MARKERS_DB}.{MARKERS_TABLE} WHERE {where}",
        parameters={"search_value": marker_path},
    )


def _query_one_duckdb(marker_path: SinkMarkerPath):
    return duckdb_local.run_query(
        query=f"SELECT marker_path FROM {MARKERS_DB}.{MARKERS_TABLE} WHERE marker_path = ?",
        params=[marker_path],
    )


def _query_many_clickhouse(datevals: list[date], chains: list[str]):
    """ClickHouse version of query many."""

    where = "dt IN {dates:Array(Date)} AND chain in {chains:Array(String)}"

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
        FROM {MARKERS_DB}.{MARKERS_TABLE}
        WHERE {where}
        """,
        parameters={"dates": datevals, "chains": chains},
    )

    # ClickHouse returns the Date type as u16 days from epoch.
    return markers.with_columns(dt=pl.from_epoch(pl.col("dt"), time_unit="d"))


def _query_many_duckdb(datevals: list[date], chains: list[str]):
    """DuckDB version of query many."""

    datelist = ", ".join([f"'{_.strftime("%Y-%m-%d")}'" for _ in datevals])
    chainlist = ", ".join(f"'{_}'" for _ in chains)

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
        FROM {MARKERS_DB}.{MARKERS_TABLE}
        WHERE dt IN ({datelist}) AND chain in ({chainlist})
        """,
    )

    return markers.pl()
