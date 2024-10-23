import os
from dataclasses import dataclass

import polars as pl
from op_coreutils import duckdb_local as utilsduckdb
from op_coreutils.clickhouse import insert_arrow, run_oplabs_query
from op_coreutils.logger import structlog
from op_coreutils.storage.gcs_parquet import (
    gcs_upload_parquet,
    local_upload_parquet,
)
from op_coreutils.storage.paths import PartitionedOutput, SinkMarkerPath

from op_datasets.etl.ingestion.markers import IngestionCompletionMarker

log = structlog.get_logger()


MARKERS_DB = "etl_monitor"
MARKERS_TABLE = "raw_onchain_ingestion_markers"


@dataclass(kw_only=True)
class DataSink:
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

    def write_marker(self, marker: IngestionCompletionMarker):
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
            insert_arrow("OPLABS", MARKERS_DB, MARKERS_TABLE, gcs_table)
            return

        elif self.is_local:
            utilsduckdb.insert_arrow(MARKERS_DB, MARKERS_TABLE, local_table)
            return

        raise NotImplementedError()

    def is_complete(self, marker_path: SinkMarkerPath) -> bool:
        if self.is_gcs:
            select = f"SELECT marker_path FROM {MARKERS_DB}.{MARKERS_TABLE} "
            result = run_oplabs_query(
                select + "WHERE marker_path = {search_value:String}",
                parameters={"search_value": marker_path},
            )
            return len(result) > 0

        elif self.is_local:
            select = f"SELECT marker_path FROM {MARKERS_DB}.{MARKERS_TABLE} "
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
