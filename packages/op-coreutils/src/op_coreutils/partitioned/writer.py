import polars as pl
import pyarrow as pa

from op_coreutils import clickhouse, duckdb_local
from op_coreutils.logger import structlog
from op_coreutils.storage.gcs_parquet import (
    gcs_upload_parquet,
    local_upload_parquet,
)

from .location import DataLocation
from .marker import MARKERS_DB, MARKERS_TABLE
from .output import WrittenParquetPath

log = structlog.get_logger()


def write_single_part(
    location: DataLocation, dataframe: pl.DataFrame, part_output: WrittenParquetPath
):
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
