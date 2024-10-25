import os
from datetime import date
from enum import Enum

import polars as pl
from op_coreutils import clickhouse, duckdb_local
from op_coreutils.partitioned import SinkMarkerPath
from op_coreutils.path import repo_path

MARKERS_DB = "etl_monitor"
MARKERS_TABLE = "raw_onchain_ingestion_markers"


class RawOnchainDataProvider(str, Enum):
    """Providers of raw onchain data."""

    GOLDSKY = "GOLDSKY"


class RawOnchainDataLocation(str, Enum):
    """Storage locations for ingested raw onchain data."""

    GCS = "GCS"
    LOCAL = "LOCAL"

    def with_prefix(self, path: str) -> str:
        if self == RawOnchainDataLocation.GCS:
            # Prepend the GCS bucket scheme and bucket name to make the paths
            # understandable by read_parquet() in DuckDB.
            return f"gs://oplabs-tools-data-sink/{path}"

        if self == RawOnchainDataLocation.LOCAL:
            # Prepend the default loal path.
            return os.path.join("ozone/warehouse", path)

    def absolute(self, path: str) -> str:
        if self == RawOnchainDataLocation.GCS:
            return self.with_prefix(path)
        if self == RawOnchainDataLocation.LOCAL:
            return os.path.abspath(repo_path(self.with_prefix(path)))


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


def markers_for_dates(
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

    assert paths_df.schema == {
        "dt": pl.Date,
        "chain": pl.String,
        "num_blocks": pl.Int32,
        "min_block": pl.Int64,
        "max_block": pl.Int64,
        "dataset_name": pl.String,
        "data_path": pl.String,
    }

    return paths_df


# Private functions


def _marker_location(data_location: RawOnchainDataLocation) -> RawOnchainMarkersLocation:
    if data_location == RawOnchainDataLocation.GCS:
        return RawOnchainMarkersLocation.OPLABS_CLICKHOUSE

    if data_location == RawOnchainDataLocation.LOCAL:
        return RawOnchainMarkersLocation.DUCKDB_LOCAL

    raise NotImplementedError(f"invalid data location: {data_location}")


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
            num_blocks,
            min_block,
            max_block,
            data_path,
            dataset_name
        FROM {MARKERS_DB}.{MARKERS_TABLE}
        WHERE {where}
        """,
        parameters={"dates": datevals, "chains": chains},
    )

    # ClickHouse returns the Date type as u16 days from epoch.
    return markers.with_columns(dt=pl.from_epoch(pl.col("dt"), time_unit="d"))


def _query_many_duckdb(datevals: list[date], chains: list[str]):
    """DuckDB version of query many."""

    datelist = ", ".join([f"'{_.strftime("%Y-%m-%d")}'" for _ in datevals])
    chainlist = ", ".join(f"'{_}'" for _ in chains)

    markers = duckdb_local.run_query(
        query=f"""
        SELECT
            dt,
            chain,
            num_blocks,
            min_block,
            max_block,
            data_path,
            dataset_name
        FROM {MARKERS_DB}.{MARKERS_TABLE}
        WHERE dt IN ({datelist}) AND chain in ({chainlist})
        """,
    )

    return markers.pl()
