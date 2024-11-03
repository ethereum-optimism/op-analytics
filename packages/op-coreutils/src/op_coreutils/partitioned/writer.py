from dataclasses import dataclass
from typing import Any, Callable

import polars as pl
import pyarrow as pa

from op_coreutils import clickhouse, duckdb_local
from op_coreutils.logger import structlog, bound_contextvars
from op_coreutils.storage.gcs_parquet import (
    gcs_upload_parquet,
    local_upload_parquet,
)

from .breakout import breakout_partitions
from .location import DataLocation
from .marker import MARKERS_DB, marker_exists, Marker
from .output import OutputDataFrame, WrittenParquetPath, ExpectedOutput
from .status import all_outputs_complete
from .types import SinkMarkerPath

log = structlog.get_logger()


@dataclass
class DataWriter:
    """Manages writing data and markers consistently."""

    # Sinks
    write_to: list[DataLocation]

    # Markers Table
    markers_table: str

    # Expected Outputs
    expected_outputs: dict[str, ExpectedOutput]

    # Is set to true if all markers already exist.
    is_complete: bool

    # If true, writes data even if markers already exist.
    force: bool

    def get_marker(self, dataset_name: str) -> SinkMarkerPath:
        """Return the marker path for the dataset name."""
        return self.expected_outputs[dataset_name].marker_path

    def all_complete(self) -> bool:
        """Check if all expected markers are complete."""
        return all_outputs_complete(
            sinks=self.write_to,
            markers=[_.marker_path for _ in self.expected_outputs.values()],
            markers_table=self.markers_table,
        )

    def write_all(
        self,
        dataframes: list[OutputDataFrame],
        basename: str,
        marker_kwargs: dict[str, Any],
    ):
        """Write data and markers to all the specified locations."""
        return self.write_all_callables(
            dataframes=[lambda: df for df in dataframes],
            basename=basename,
            marker_kwargs=marker_kwargs,
        )

    def write_all_callables(
        self,
        dataframes: list[Callable[[], OutputDataFrame]],
        basename: str,
        marker_kwargs: dict[str, Any],
    ):
        """Write data and markers to all the specified locations.

        The data is provided as a list of functions that return a dataframe. This lets us generalize
        the way in which different tasks produce OutputDataFrame.
        """
        for location in self.write_to:
            for func in dataframes:
                output_dataframe: OutputDataFrame = func()

                # The default partition value is included in logs because it includes
                # the dt value, which helps keep track of where we are when we run a
                # backfill.
                with bound_contextvars(**output_dataframe.default_partition):
                    marker_path = self.get_marker(output_dataframe.dataset_name)

                    is_complete = marker_exists(
                        data_location=location,
                        marker_path=marker_path,
                        markers_table=self.markers_table,
                    )

                    if is_complete and not self.force:
                        log.info(
                            f"[{location.name}] Skipping already complete output at {marker_path}"
                        )
                        continue

                    written_parts: list[WrittenParquetPath] = []
                    parts = breakout_partitions(
                        df=output_dataframe.dataframe,
                        partition_cols=["chain", "dt"],
                        root_path=output_dataframe.root_path,
                        basename=basename,
                        default_partition=output_dataframe.default_partition,
                    )

                    for part_df, part in parts:
                        write_single_part(
                            location=location,
                            dataframe=part_df,
                            part_output=part,
                        )
                        written_parts.append(part)

                    marker = Marker(
                        marker_path=marker_path,
                        dataset_name=output_dataframe.dataset_name,
                        root_path=output_dataframe.root_path,
                        data_paths=written_parts,
                        **marker_kwargs,
                    )

                    write_marker(
                        data_location=location,
                        arrow_table=marker.to_pyarrow_table(),
                        markers_table=self.markers_table,
                    )
                    log.info(f"Wrote {output_dataframe.dataset_name} to {location.name}")


def write_single_part(
    location: DataLocation,
    dataframe: pl.DataFrame,
    part_output: WrittenParquetPath,
):
    """Write a single parquet output file for a partitioned output."""
    if location == DataLocation.GCS:
        gcs_upload_parquet(part_output.full_path, dataframe)
        return

    elif location == DataLocation.LOCAL:
        local_upload_parquet(
            path=location.with_prefix(part_output.full_path),
            df=dataframe,
        )

        return

    raise NotImplementedError()


def write_marker(
    data_location: DataLocation,
    arrow_table: pa.Table,
    markers_table: str,
):
    """Write marker.

    Having markers allows us to quickly check completion and perform analytics
    over previous iterations of the ingestion process.

    Markers for GCS output are written to Clickhouse.
    Markers for local output are written to DuckDB

    """
    if data_location == DataLocation.GCS:
        clickhouse.insert_arrow("OPLABS", MARKERS_DB, markers_table, arrow_table)
        return

    elif data_location == DataLocation.LOCAL:
        duckdb_local.insert_arrow(MARKERS_DB, markers_table, arrow_table)
        return

    raise NotImplementedError()
