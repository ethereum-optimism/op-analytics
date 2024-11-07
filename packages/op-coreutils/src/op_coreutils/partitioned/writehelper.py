from dataclasses import dataclass
from typing import Any

import polars as pl
from overrides import EnforceOverrides, override

from op_coreutils import clickhouse, duckdb_local
from op_coreutils.logger import structlog
from op_coreutils.storage.gcs_parquet import (
    gcs_upload_parquet,
    local_upload_parquet,
)

from .breakout import breakout_partitions
from .location import DataLocation
from .marker import MARKERS_DB, marker_exists, Marker
from .output import OutputData, ExpectedOutput, OutputPartMeta

log = structlog.get_logger()


@dataclass
class WriteManager(EnforceOverrides):
    """Helper class that allows arbitrary write logic and handles completion markers.

    - If complention markers exist no data is written.
    - If data is written correctly commpletion markers are created.
    """

    location: DataLocation
    expected_output: ExpectedOutput
    markers_table: str
    force: bool

    def write_implementation(self, output_data: Any) -> list[OutputPartMeta]:
        raise NotImplementedError()

    def write(self, output_data: Any):
        is_complete = marker_exists(
            data_location=self.location,
            marker_path=self.expected_output.marker_path,
            markers_table=self.markers_table,
        )

        if is_complete and not self.force:
            log.info(
                f"[{self.location.name}] Skipping already complete output at {self.expected_output.marker_path}"
            )
            return

        written_parts = self.write_implementation(output_data)

        write_marker(
            data_location=self.location,
            expected_output=self.expected_output,
            written_parts=written_parts,
            markers_table=self.markers_table,
        )
        log.info(f"Wrote {self.expected_output.dataset_name} to {self.location.name}")


class ParqueWriteManager(WriteManager):
    @override
    def write_implementation(self, output_data: Any) -> list[OutputPartMeta]:
        assert isinstance(output_data, OutputData)

        parts = breakout_partitions(
            df=output_data.dataframe,
            partition_cols=["chain", "dt"],
            default_partition=output_data.default_partition,
        )

        parts_meta: list[OutputPartMeta] = []
        for part in parts:
            write_single_part(
                location=self.location,
                dataframe=part.df,
                full_path=part.meta.full_path(
                    self.expected_output.root_path, self.expected_output.file_name
                ),
            )
            parts_meta.append(part.meta)

        return parts_meta


def write_single_part(
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
        clickhouse.insert_arrow("OPLABS", MARKERS_DB, markers_table, arrow_table)
        return

    elif data_location == DataLocation.LOCAL:
        duckdb_local.insert_arrow(MARKERS_DB, markers_table, arrow_table)
        return

    raise NotImplementedError()
