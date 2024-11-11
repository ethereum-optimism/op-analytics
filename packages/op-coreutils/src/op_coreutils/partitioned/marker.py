import socket
from dataclasses import dataclass
from datetime import date

import polars as pl
import pyarrow as pa

from op_coreutils import clickhouse, duckdb_local
from op_coreutils.time import date_fromstr, now

from .location import DataLocation, MarkersLocation, marker_location
from .output import OutputPartMeta, ExpectedOutput
from .types import SinkMarkerPath


@dataclass
class Marker:
    """Represent a marker for a collection of objects written to storage."""

    expected_output: ExpectedOutput

    written_parts: list[OutputPartMeta]

    @property
    def marker_path(self):
        return self.expected_output.marker_path

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
            + self.expected_output.additional_columns_schema
        )

    def to_pyarrow_table(self) -> pa.Table:
        schema = self.arrow_schema()

        current_time = now()
        hostname = socket.gethostname()
        rows = []
        for parquet_out in self.written_parts:
            parquet_out_row = {
                "updated_at": current_time,
                "marker_path": self.expected_output.marker_path,
                "root_path": self.expected_output.root_path,
                "num_parts": len(self.written_parts),
                "dataset_name": self.expected_output.dataset_name,
                "data_path": parquet_out.full_path(
                    self.expected_output.root_path, self.expected_output.file_name
                ),
                "row_count": parquet_out.row_count,
                "process_name": self.expected_output.process_name,
                "writer_name": hostname,
            }

            for partition in parquet_out.partitions:
                sch = schema.field(partition.key)
                if sch.type == pa.date32():
                    if isinstance(partition.value, str):
                        parquet_out_row[partition.key] = date_fromstr(partition.value)
                    elif isinstance(partition.value, date):
                        parquet_out_row[partition.key] = partition.value
                    else:
                        raise NotImplementedError(
                            f"unsupported value for date partition: {partition.value}"
                        )
                else:
                    parquet_out_row[partition.key] = partition.value

            rows.append(parquet_out_row)

        for row in rows:
            for name, value in self.expected_output.additional_columns.items():
                row[name] = value

        return pa.Table.from_pylist(rows, schema=schema)


MARKERS_DB = "etl_monitor"


def marker_exists(
    data_location: DataLocation,
    marker_path: SinkMarkerPath,
    markers_table: str,
) -> bool:
    """Run a query to find if a marker already exists."""
    store = marker_location(data_location)

    if store == MarkersLocation.OPLABS_CLICKHOUSE:
        result = _query_one_clickhouse(marker_path, markers_table)
    else:
        # default to DUCKDB_LOCAL
        result = _query_one_duckdb(marker_path, markers_table)

    return len(result) > 0


def markers_for_dates(
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
        paths_df = _query_many_clickhouse(datevals, chains, markers_table, dataset_names)
    else:
        # default to DUCKDB_LOCAL
        paths_df = _query_many_duckdb(datevals, chains, markers_table, dataset_names)

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


def _query_one_clickhouse(marker_path: SinkMarkerPath, markers_table: str):
    where = "marker_path = {search_value:String}"

    return clickhouse.run_oplabs_query(
        query=f"SELECT marker_path FROM {MARKERS_DB}.{markers_table} WHERE {where}",
        parameters={"search_value": marker_path},
    )


def _query_one_duckdb(marker_path: SinkMarkerPath, markers_table: str):
    return duckdb_local.run_query(
        query=f"SELECT marker_path FROM {MARKERS_DB}.{markers_table} WHERE marker_path = ?",
        params=[marker_path],
    )


def _query_many_clickhouse(
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
