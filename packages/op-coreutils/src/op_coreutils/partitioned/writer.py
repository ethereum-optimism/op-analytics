from dataclasses import dataclass
from typing import Any

import polars as pl
import pyarrow as pa

from op_coreutils import clickhouse, duckdb_local
from op_coreutils.logger import structlog
from op_coreutils.storage.gcs_parquet import (
    gcs_upload_parquet,
    local_upload_parquet,
)

from .breakout import breakout_partitions
from .location import DataLocation
from .marker import MARKERS_DB, MARKERS_TABLE, marker_exists, Marker
from .output import WrittenParquetPath
from .types import SinkMarkerPath, SinkOutputRootPath

log = structlog.get_logger()


@dataclass
class OutputDataFrame:
    dataframe: pl.DataFrame
    root_path: SinkOutputRootPath
    marker_path: SinkMarkerPath
    dataset_name: str

    # Default partition values for cases when the output datafarame is empty
    # and therefore has no implicit partition values.
    default_partition: dict[str, Any]


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


def write_marker(location: DataLocation, arrow_table: pa.Table):
    """Write marker.

    Having markers allows us to quickly check completion and perform analytics
    over previous iterations of the ingestion process.

    Markers for GCS output are written to Clickhouse.
    Markers for local output are written to DuckDB

    """
    if location == DataLocation.GCS:
        clickhouse.insert_arrow("OPLABS", MARKERS_DB, MARKERS_TABLE, arrow_table)
        return

    elif location == DataLocation.LOCAL:
        duckdb_local.insert_arrow(MARKERS_DB, MARKERS_TABLE, arrow_table)
        return

    raise NotImplementedError()


def write_all(
    locations: list[DataLocation],
    dataframes: list[OutputDataFrame],
    basename: str,
    marker_kwargs: dict[str, Any],
    force: bool = False,
):
    """Write dataframes to all the specified locations."""
    for location in locations:
        for output in dataframes:
            is_complete = marker_exists(location, output.marker_path)

            if is_complete and not force:
                log.info(
                    f"[{location.name}] Skipping already complete output at {output.marker_path}"
                )
                continue

            written_parts: list[WrittenParquetPath] = []
            parts = breakout_partitions(
                df=output.dataframe,
                partition_cols=["chain", "dt"],
                root_path=output.root_path,
                basename=basename,
                default_partition=output.default_partition,
            )

            for part_df, part in parts:
                write_single_part(
                    location=location,
                    dataframe=part_df,
                    part_output=part,
                )
                written_parts.append(part)

            marker = Marker(
                marker_path=output.marker_path,
                dataset_name=output.dataset_name,
                root_path=output.root_path,
                data_paths=written_parts,
                **marker_kwargs,
            )

            write_marker(
                location=location,
                arrow_table=marker.to_pyarrow_table(),
            )
            log.info(f"Wrote {output.dataset_name} to {location.name}")
