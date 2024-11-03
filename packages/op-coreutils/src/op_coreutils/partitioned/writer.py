from typing import Any

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
from .output import WrittenParquetPath
from .output import OutputDataFrame

log = structlog.get_logger()


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


def write_all(
    locations: list[DataLocation],
    dataframes: list[OutputDataFrame],
    basename: str,
    markers_table: str,
    marker_kwargs: dict[str, Any],
    force: bool = False,
):
    """Write dataframes to all the specified locations."""
    for location in locations:
        for output_dataframe in dataframes:
            # The default partition value is included in logs because it includes
            # the dt value, which helps keep track of where we are when we run a
            # backfill.
            with bound_contextvars(**output_dataframe.default_partition):
                is_complete = marker_exists(
                    data_location=location,
                    marker_path=output_dataframe.marker_path,
                    markers_table=output_dataframe.markers_table,
                )

                if is_complete and not force:
                    log.info(
                        f"[{location.name}] Skipping already complete output at {output_dataframe.marker_path}"
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
                    marker_path=output_dataframe.marker_path,
                    dataset_name=output_dataframe.dataset_name,
                    root_path=output_dataframe.root_path,
                    data_paths=written_parts,
                    **marker_kwargs,
                )

                write_marker(
                    data_location=location,
                    arrow_table=marker.to_pyarrow_table(),
                    markers_table=markers_table,
                )
                log.info(f"Wrote {output_dataframe.dataset_name} to {location.name}")
