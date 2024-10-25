from dataclasses import dataclass

import polars as pl
from op_coreutils import duckdb_local as utilsduckdb
from op_coreutils.clickhouse import insert_arrow
from op_coreutils.logger import structlog
from op_coreutils.storage.gcs_parquet import (
    gcs_upload_parquet,
    local_upload_parquet,
)
from op_coreutils.storage.paths import PartitionedOutput, SinkMarkerPath

from op_datasets.etl.ingestion.markers import IngestionCompletionMarker

from .utilities import MARKERS_DB, MARKERS_TABLE, RawOnchainDataLocation, marker_exists

log = structlog.get_logger()


@dataclass(kw_only=True)
class RawOnchainDataSink:
    location: RawOnchainDataLocation

    def write_single_part(self, dataframe: pl.DataFrame, part_output: PartitionedOutput):
        if self.location == RawOnchainDataLocation.GCS:
            gcs_upload_parquet(part_output.path.full_path, dataframe)
            return

        elif self.location == RawOnchainDataLocation.LOCAL:
            local_upload_parquet(
                path=self.location.with_prefix(part_output.path.full_path),
                df=dataframe,
            )

            return

        raise NotImplementedError()

    def write_marker(self, marker: IngestionCompletionMarker):
        """Write marker.

        Having markers allows us to quickly check completion and perform analytics
        over previous iterations of the ingestion process.

        Markers for GCS output are written to Clickhouse.
        Markers for local output are written to DuckDB

        """
        table = marker.to_pyarrow_table()

        if self.location == RawOnchainDataLocation.GCS:
            insert_arrow("OPLABS", MARKERS_DB, MARKERS_TABLE, table)
            return

        elif self.location == RawOnchainDataLocation.LOCAL:
            utilsduckdb.insert_arrow(MARKERS_DB, MARKERS_TABLE, table)
            return

        raise NotImplementedError()

    def is_complete(self, marker_path: SinkMarkerPath) -> bool:
        return marker_exists(self.location, marker_path)
