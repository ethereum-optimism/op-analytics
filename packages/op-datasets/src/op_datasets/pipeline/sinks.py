import os
from dataclasses import dataclass

import polars as pl
import pyarrow as pa
from op_coreutils import duckdb as utilsduckdb
from op_coreutils.clickhouse.client import insert_arrow, run_oplabs_query
from op_coreutils.logger import structlog
from op_coreutils.storage.gcs_parquet import (
    gcs_upload_parquet,
    local_upload_parquet,
)
from op_coreutils.storage.paths import Marker, PartitionedOutput, SinkMarkerPath
from op_coreutils.time import now
from overrides import EnforceOverrides

log = structlog.get_logger()


@dataclass
class IngestionProcessMarker:
    process_name: str
    chain: str
    marker: Marker

    @property
    def path(self):
        return self.marker.marker_path

    def dt_value(self):
        return min(_.path.dt_value() for _ in self.marker.outputs)

    def to_clickhouse_pyarrow_table(self) -> pa.Table:
        row: dict = self.marker.to_clickhouse_row()
        row["process_name"] = self.process_name
        row["chain"] = self.chain
        row["dt"] = self.dt_value()

        marker_schema = self.marker.clickhouse_schema()
        schema = pa.schema(
            [
                marker_schema.field("marker_path"),
                marker_schema.field("total_rows"),
                marker_schema.field("outputs.full_path"),
                marker_schema.field("outputs.partition_cols"),
                marker_schema.field("outputs.row_count"),
                pa.field("process_name", pa.string()),
                pa.field("chain", pa.string()),
                pa.field("dt", pa.string()),
            ]
        )

        return pa.Table.from_pylist(
            [row],
            schema=schema,
        )

    def to_duckdb_pyarrow_table(self) -> pa.Table:
        row: dict = self.marker.to_duckdb_row()
        row["updated_at"] = now()
        row["process_name"] = self.process_name
        row["chain"] = self.chain
        row["dt"] = self.dt_value()

        marker_schema = self.marker.duckdb_schema()

        # The order of the columns must match the order as declared in the CREATE TABLE
        # statement for the table.
        schema = pa.schema(
            [
                pa.field("updated_at", pa.timestamp(unit="us", tz=None)),
                marker_schema.field("marker_path"),
                marker_schema.field("total_rows"),
                marker_schema.field("outputs"),
                pa.field("process_name", pa.string()),
                pa.field("chain", pa.string()),
                pa.field("dt", pa.string()),
            ]
        )

        return pa.Table.from_pylist(
            [row],
            schema=schema,
        )


@dataclass(kw_only=True)
class DataSink(EnforceOverrides):
    sink_spec: str

    def local_full_path(self, path: str):
        return os.path.join(self.sink_spec.removeprefix("file://"), path)

    @property
    def is_gcs(self):
        return self.sink_spec == "gcs"

    @property
    def is_local(self):
        return self.sink_spec.startswith("file://")

    def write_single_part(self, dataframe: pl.DataFrame, part_output: PartitionedOutput):
        if self.is_gcs:
            gcs_upload_parquet(part_output.path.full_path, dataframe)
            return
        elif self.is_local:
            local_upload_parquet(self.local_full_path(part_output.path.full_path), dataframe)
            return

        raise NotImplementedError()

    MARKERS_DB = "etl_monitor"
    MARKERS_TABLE = "raw_onchain_ingestion_markers"

    def write_marker(self, marker: IngestionProcessMarker):
        """Write marker.

        Having markers allows us to quickly check completion and perform analytics
        over previous iterations of the ingestion process.

        Markers for GCS output are written to Clickhouse.
        Markers for local output are written to DuckDB

        """
        # Note that the marker schemas are slightly different in Clickhouse and DuckDB. This is
        # due to the difference in how these database support the nested structs that we use to
        # represent partition values.
        #
        # We create both flavors of the markers arrow table just so we can exercise the GCS code
        # when we are testing locally.

        gcs_table = marker.to_clickhouse_pyarrow_table()
        local_table = marker.to_duckdb_pyarrow_table()

        if self.is_gcs:
            insert_arrow("OPLABS", self.MARKERS_DB, self.MARKERS_TABLE, gcs_table)
            return

        elif self.is_local:
            utilsduckdb.insert_arrow(self.MARKERS_DB, self.MARKERS_TABLE, local_table)
            return

        raise NotImplementedError()

    def is_complete(self, marker_path: SinkMarkerPath) -> bool:
        if self.is_gcs:
            select = f"SELECT marker_path FROM {self.MARKERS_DB}.{self.MARKERS_TABLE} "
            result = run_oplabs_query(
                select + "WHERE marker_path = {search_value:String}",
                parameters={"search_value": marker_path},
            )
            return len(result) > 0

        elif self.is_local:
            select = f"SELECT marker_path FROM {self.MARKERS_DB}.{self.MARKERS_TABLE} "
            result = utilsduckdb.run_sql(
                sql=select + "WHERE marker_path = ?",
                params=[marker_path],
            )
            return len(result) > 0

        raise NotImplementedError


def all_outputs_complete(sinks: list[DataSink], markers: list[SinkMarkerPath]) -> bool:
    result = True
    for sink in sinks:
        complete = []
        incomplete = []
        for marker in markers:
            if sink.is_complete(marker):
                complete.append(marker)
            else:
                incomplete.append(marker)

        log.info(
            f"{len(complete)} complete, {len(incomplete)} incomplete locations out of {len(markers)} expected at {sink.sink_spec}"
        )

        if incomplete:
            log.info(f"Showing the first 5 incomplete locations at {sink.sink_spec}")
            for location in sorted(incomplete)[:5]:
                log.info(f"DataSink {sink.sink_spec!r} is incomplete at {location!r}")
            result = False

    return result
