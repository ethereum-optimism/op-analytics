from datetime import date
from typing import Literal

import polars as pl
from op_coreutils.clickhouse import run_goldsky_query, run_oplabs_query

from op_datasets.schemas import ONCHAIN_CURRENT_VERSION
from op_datasets.utils.blockrange import BlockRange
from op_datasets.utils.daterange import DateRange
from op_coreutils.storage.paths import SinkMarkerPath
from op_coreutils import duckdb_local


MARKERS_DB = "etl_monitor"
MARKERS_TABLE = "raw_onchain_ingestion_markers"


def block_range_for_dates(chain: str, date_spec: str):
    """Find the block range required to cover the provided dates.

    Uses the raw blocks dataset in Goldsky Clickhouse to find out which blocks have
    timestamps in the required dates.
    """
    date_range = DateRange.from_spec(date_spec)

    if len(date_range) > 2:
        raise ValueError("We don't recommend processing so much data at once.")

    query = ONCHAIN_CURRENT_VERSION["blocks"].goldsky_sql_find_blocks_for_dates(
        source_table=f"{chain}_blocks",
    )

    params = {
        "mints": date_range.min_ts,
        "maxts": date_range.max_ts,
    }

    result = run_goldsky_query(query=query, parameters=params)
    assert len(result) == 1

    row = result.to_dicts()[0]
    return BlockRange(row["block_min"], row["block_max"])


def next_batches():
    """Find a list of batches that are ready to be ingested.

    Looks at the last 48 hours and sweeps to make sure we have all blocks.
    """


def marker_exists(
    marker_storage: Literal["OPLABS_CLICKHOUSE", "LOCAL_DUCKDB"],
    marker_path: SinkMarkerPath,
) -> bool:
    """Run a query to find if a marker already exists."""
    if marker_storage == "OPLABS_CLICKHOUSE":
        result = _query_one_clickhouse(marker_path)
    elif marker_storage == "LOCAL_DUCKDB":
        result = _query_one_duckdb(marker_path)
    else:
        raise NotImplementedError

    return len(result) > 0


def marker_paths_for_dates(
    marker_storage: Literal["OPLABS_CLICKHOUSE", "LOCAL_DUCKDB"],
    datevals: list[date],
    chains: list[str],
) -> pl.DataFrame:
    """Query completion markers for a list of dates and chains.

    Returns a dataframe with the markers and all of the parquet output paths
    associated with them.
    """

    if marker_storage == "OPLABS_CLICKHOUSE":
        paths_df = _query_many_clickhouse(datevals, chains)
    elif marker_storage == "LOCAL_DUCKDB":
        paths_df = _query_many_duckdb(datevals, chains)

    paths_by_dataset_df = (
        paths_df.with_columns(
            dataset=pl.col("parquet_path").map_elements(
                _marker_path_to_dataset,
                return_dtype=pl.String,
            )
        )
        .group_by("chain", "dataset")
        .agg(pl.col("parquet_path"))
    )

    assert paths_by_dataset_df.schema == {
        "chain": pl.String,
        "dataset": pl.String,
        "parquet_path": pl.List(pl.String),
    }
    return paths_by_dataset_df


# Private functions


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

    return run_oplabs_query(
        query=f"SELECT marker_path FROM {MARKERS_DB}.{MARKERS_TABLE} WHERE {where}",
        parameters={"search_value": marker_path},
    )


def _query_one_duckdb(marker_path: SinkMarkerPath):
    return duckdb_local.run_sql(
        sql=f"SELECT marker_path FROM {MARKERS_DB}.{MARKERS_TABLE} WHERE marker_path = ?",
        params=[marker_path],
    )


def _query_many_clickhouse(datevals: list[date], chains: list[str]):
    """ClickHouse version of query many."""

    where = "dt IN {dates:Array(Date)} AND chain in {chains:Array(String)}"

    markers = run_oplabs_query(
        query=f"""
        SELECT
            marker_path, 
            outputs.full_path, chain 
        FROM {MARKERS_DB}.{MARKERS_TABLE}
        WHERE {where}
        """,
        parameters={"dates": datevals, "chains": chains},
    )

    parquet_paths = markers.select(
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
    pass
