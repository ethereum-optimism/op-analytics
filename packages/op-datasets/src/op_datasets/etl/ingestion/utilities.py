import os
from datetime import date
from enum import Enum

import polars as pl
from op_coreutils import clickhouse, duckdb_local
from op_coreutils.storage.paths import SinkMarkerPath

MARKERS_DB = "etl_monitor"
MARKERS_TABLE = "raw_onchain_ingestion_markers"


class RawOnchainDataProvider(str, Enum):
    """Providers of raw onchain data."""

    GOLDSKY = "GOLDSKY"


class RawOnchainDataLocation(str, Enum):
    """Storage locations for ingested raw onchain data."""

    GCS = "GCS"
    LOCAL = "LOCAL"


# Hard-coded path for local data outputs.
LOCAL_PATH_PREFIX = "ozone/warehouse"


def local_path(path: str):
    return os.path.join(LOCAL_PATH_PREFIX, path)


class RawOnchainMarkersLocation(str, Enum):
    """Storage locations for ingestion process markers."""

    OPLABS_CLICKHOUSE = "OPLABS_CLICKHOUSE"
    DUCKDB_LOCAL = "DUCKDB_LOCAL"


def marker_exists(
    data_location: RawOnchainDataLocation,
    marker_path: SinkMarkerPath,
) -> bool:
    """Run a query to find if a marker already exists."""
    store = _marker_location(data_location)

    if store == RawOnchainMarkersLocation.OPLABS_CLICKHOUSE:
        result = _query_one_clickhouse(marker_path)
    else:
        # default to DUCKDB_LOCAL
        result = _query_one_duckdb(marker_path)

    return len(result) > 0


def marker_paths_for_dates(
    data_location: RawOnchainDataLocation,
    datevals: list[date],
    chains: list[str],
) -> pl.DataFrame:
    """Query completion markers for a list of dates and chains.

    Returns a dataframe with the markers and all of the parquet output paths
    associated with them.
    """
    store = _marker_location(data_location)

    if store == RawOnchainMarkersLocation.OPLABS_CLICKHOUSE:
        paths_df = _query_many_clickhouse(datevals, chains)
    else:
        # default to DUCKDB_LOCAL
        paths_df = _query_many_duckdb(datevals, chains)

    paths_by_dataset_df = (
        paths_df.with_columns(
            dataset=pl.col("parquet_path").map_elements(
                _marker_path_to_dataset,
                return_dtype=pl.String,
            )
        )
        .group_by("dt", "chain", "dataset")
        .agg(pl.col("parquet_path"))
    )

    assert paths_by_dataset_df.schema == {
        "dt": pl.Date,
        "chain": pl.String,
        "dataset": pl.String,
        "parquet_path": pl.List(pl.String),
    }
    return paths_by_dataset_df


# Private functions


def _marker_location(data_location: RawOnchainDataLocation) -> RawOnchainMarkersLocation:
    if data_location == RawOnchainDataLocation.GCS:
        return RawOnchainMarkersLocation.OPLABS_CLICKHOUSE

    if data_location == RawOnchainDataLocation.LOCAL:
        return RawOnchainMarkersLocation.DUCKDB_LOCAL

    raise NotImplementedError(f"invalid data location: {data_location}")


def _marker_path_to_dataset(path: str) -> str:
    if "ingestion/blocks" in path:
        return "blocks"
    if "ingestion/transactions" in path:
        return "transactions"
    if "ingestion/logs" in path:
        return "logs"
    if "ingestion/traces" in path:
        return "traces"

    raise NotImplementedError()


def _query_one_clickhouse(marker_path: SinkMarkerPath):
    where = "marker_path = {search_value:String}"

    return clickhouse.run_oplabs_query(
        query=f"SELECT marker_path FROM {MARKERS_DB}.{MARKERS_TABLE} WHERE {where}",
        parameters={"search_value": marker_path},
    )


def _query_one_duckdb(marker_path: SinkMarkerPath):
    return duckdb_local.run_query(
        query=f"SELECT marker_path FROM {MARKERS_DB}.{MARKERS_TABLE} WHERE marker_path = ?",
        params=[marker_path],
    )


def _query_many_clickhouse(datevals: list[date], chains: list[str]):
    """ClickHouse version of query many."""

    where = "dt IN {dates:Array(Date)} AND chain in {chains:Array(String)}"

    markers = clickhouse.run_oplabs_query(
        query=f"""
        SELECT
            dt,
            chain,
            outputs.full_path,
        FROM {MARKERS_DB}.{MARKERS_TABLE}
        WHERE {where}
        """,
        parameters={"dates": datevals, "chains": chains},
    )

    parquet_paths = markers.select(
        pl.col("dt"),
        pl.col("chain"),
        pl.col("outputs.full_path").alias("parquet_path"),
    ).explode("parquet_path")

    def _prefix(x):
        return f"gs://oplabs-tools-data-sink/{x}"

    # Prepend the GCS bucket scheme and bucket name to make the paths
    # understandable by read_parquet() in DuckDB.
    return parquet_paths.with_columns(
        parquet_path=pl.col("parquet_path").map_elements(
            _prefix,
            return_dtype=pl.String,
        )
    )


def _query_many_duckdb(datevals: list[date], chains: list[str]):
    """DuckDB version of query many."""

    markers = duckdb_local.run_query(
        query=f"""
        WITH exploded AS (
            SELECT
                dt,
                chain,
                unnest(outputs) as output
            FROM {MARKERS_DB}.{MARKERS_TABLE}
            WHERE dt IN ?
        )
        
        SELECT
            dt,
            chain,
            output.full_path AS parquet_path
        FROM exploded
        """,
        params=[datevals],
    )

    parquet_paths = markers.pl()

    return parquet_paths.with_columns(
        parquet_path=pl.col("parquet_path").map_elements(
            local_path,
            return_dtype=pl.String,
        )
    )
