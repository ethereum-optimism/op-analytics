from dataclasses import dataclass

import polars as pl

from op_analytics.coreutils.bigquery.write import most_recent_dates
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydatautils import dt_summary
from op_analytics.coreutils.request import get_data, new_session
from op_analytics.coreutils.threads import run_concurrently
from op_analytics.coreutils.time import now_dt, dt_fromepoch

from .dataaccess import DefiLlama

log = structlog.get_logger()

YIELD_POOLS_ENDPOINT = "https://yields.llama.fi/pools"
YIELD_POOL_CHART_ENDPOINT = "https://yields.llama.fi/chart/{pool}"

YIELD_TABLE_LAST_N_DAYS = 7


@dataclass
class DefillamaYield:
    """Metadata and yield data for all pools.

    This is the result we obtain after fetching from the API and extracting the data
    that we need to ingest.
    """

    pool_yield_df: pl.DataFrame


def pull_yield_data(pull_pools: list[str] | None = None) -> pl.DataFrame:
    """
    Pulls and processes yield pool data from DeFiLlama.

    Args:
        pull_pools: list of pool IDs to process. Defaults to None (process all).

    Returns:
        A polars DataFrame containing joined pool and historical yield data.
    """
    session = new_session()

    # Get all pools data
    pools_data = get_data(session, YIELD_POOLS_ENDPOINT)
    pools_df = extract_pools_data(pools_data["data"])

    # Write pools metadata
    DefiLlama.YIELD_POOLS_METADATA.write(
        dataframe=pools_df.with_columns(dt=pl.lit(now_dt())),
        sort_by=["pool"],
    )

    # Get pool IDs to process
    pool_ids = pools_df["pool"].to_list() if pull_pools is None else pull_pools

    # Call the API endpoint for each pool in parallel
    urls = {pool: YIELD_POOL_CHART_ENDPOINT.format(pool=pool) for pool in pool_ids}
    historical_yield_data = run_concurrently(lambda x: get_data(session, x), urls, max_workers=4)

    # Extract historical yield data
    historical_yield_df = extract_historical_yield_data(historical_yield_data)

    # Merge historical data with pool metadata
    pool_yield_df = historical_yield_df.join(pools_df, on="pool", how="left")

    # Write yield data
    DefiLlama.HISTORICAL_YIELD.write(
        dataframe=most_recent_dates(
            pool_yield_df, n_dates=YIELD_TABLE_LAST_N_DAYS, date_column="dt"
        ),
        sort_by=["dt", "chain", "protocol_slug", "pool"],
    )

    return DefillamaYield(pool_yield_df=pool_yield_df)


def execute_pull():
    result = pull_yield_data()
    return {"pool_yield_df": dt_summary(result.pool_yield_df)}


def extract_pools_data(pools_data: list) -> pl.DataFrame:
    """Extract pools data and transform into dataframe"""
    records = [
        {
            "pool": pool["pool"],
            "protocol_slug": pool["project"],
            "chain": pool["chain"],
            "symbol": pool["symbol"],
            "underlying_tokens": pool.get("underlyingTokens", []),
        }
        for pool in pools_data
    ]

    return pl.DataFrame(records)


def extract_historical_yield_data(data: dict) -> pl.DataFrame:
    """Extract historical yield data and transform into dataframe"""
    records = [
        {
            "pool": pool_id,
            "dt": dt_fromepoch(entry["timestamp"]),
            "tvl": entry.get("tvl", 0.0),
            "apy": entry.get("apy", 0.0),
        }
        for pool_id, pool_data in data.items()
        for entry in pool_data["data"]
    ]

    return pl.DataFrame(records)
