import json
import os

from dataclasses import dataclass

import duckdb
import pyarrow as pa
import polars as pl
from overrides import EnforceOverrides, override

from op_coreutils.path import repo_path
from op_coreutils.clickhouse.client import insert_arrow, run_oplabs_query
from op_coreutils.logger import structlog
from op_coreutils.storage.gcs_parquet import (
    gcs_upload_parquet,
    local_upload_parquet,
)
from op_coreutils.storage.paths import SinkMarkerPath, PartitionedOutput, Marker
from fsspec.implementations.local import LocalFileSystem


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

    def to_row(self) -> dict:
        row: dict = self.marker.to_clickhouse_row()
        row["process_name"] = self.process_name
        row["chain"] = self.chain
        row["dt"] = self.dt_value()
        return row

    def to_pyarrow_table(self) -> pa.Table:
        row: dict = self.to_row()

        schema = self.marker.clickhouse_schema()
        schema.append(pa.field("process_name", pa.string()))
        schema.append(pa.field("chain", pa.string()))
        schema.append(pa.field("dt", pa.string()))

        return pa.Table.from_pylist(
            [row],
            schema=schema,
        )


@dataclass(kw_only=True)
class DataSink(EnforceOverrides):
    sink_spec: str
    completion_status_cache: dict[SinkMarkerPath, bool]

    @classmethod
    def from_spec(cls, sink_spec: str) -> "DataSink":
        if sink_spec == "gcs":
            return GCSSink(
                sink_spec=sink_spec,
                completion_status_cache=dict(),
            )

        if sink_spec.startswith("file://"):
            return LocalFileSink(
                sink_spec=sink_spec,
                completion_status_cache=dict(),
                basepath=sink_spec.removeprefix("file://"),
            )

        raise NotImplementedError()

    def write_single_part(self, dataframe: pl.DataFrame, part_output: PartitionedOutput):
        raise NotImplementedError()

    def write_marker(self, marker: IngestionProcessMarker):
        raise NotImplementedError()

    def is_complete(self, marker_path: SinkMarkerPath) -> bool:
        raise NotImplementedError


@dataclass(kw_only=True)
class GCSSink(DataSink):
    @override
    def write_single_part(self, dataframe: pl.DataFrame, part_output: PartitionedOutput):
        gcs_upload_parquet(part_output.path.full_path, dataframe)

    @override
    def write_marker(self, marker: IngestionProcessMarker):
        """Markers for GCS output are writen to Clickhouse.

        Having markers in clickhouse allows us to quickly perform analytics
        and query marker data over time.
        """
        table = marker.to_pyarrow_table()
        insert_arrow("OPLABS", "etl_monitor", "raw_onchain_ingestion_markers", table)

    @override
    def is_complete(self, marker_path: SinkMarkerPath) -> bool:
        result = run_oplabs_query(
            "SELECT marker_path FROM etl_monitor.raw_onchain_ingestion_markers WHERE marker_path = {search_value:String}",
            parameters={"search_value": marker_path},
        )

        return len(result) > 0


@dataclass(kw_only=True)
class LocalFileSink(DataSink):
    basepath: str

    def full_path(self, path: str):
        return os.path.join(self.basepath, path)

    MARKERS_TABLE = "raw_onchain_ingestion_markers"

    @override
    def write_single_part(self, dataframe: pl.DataFrame, part_output: PartitionedOutput):
        local_upload_parquet(self.full_path(part_output.path.full_path), dataframe)

    def connect(self):
        path = self.full_path("markers/etl_monitor.db")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        return duckdb.connect(self.full_path("markers/etl_monitor.db"))

    def create_sql(self):
        with open(repo_path(f"ddl/duckdb_local/etl_monitor.{self.MARKERS_TABLE}.sql"), "r") as fobj:
            return fobj.read()

    @override
    def write_marker(self, marker: IngestionProcessMarker):
        con = self.connect()
        con.sql(self.create_sql())

        arrow_table = marker.to_pyarrow_table()  # noqa: F841
        con.sql(f"INSERT INTO {self.MARKERS_TABLE} SELECT * FROM arrow_table")

        row: dict = marker.to_row()
        fs = LocalFileSystem(auto_mkdir=True)
        with fs.open(self.full_path(marker.path), "w") as fobj:
            fobj.write(json.dumps(row, indent=2))

    @override
    def is_complete(self, marker_path: SinkMarkerPath) -> bool:
        con = self.connect()
        con.sql(self.create_sql())
        if os.path.isfile(self.full_path(marker_path)):
            return True
        else:
            return False


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
