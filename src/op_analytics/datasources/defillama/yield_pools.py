from dataclasses import dataclass

import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydatautils import dt_summary
from op_analytics.coreutils.request import get_data, new_session
from op_analytics.coreutils.threads import run_concurrently
from op_analytics.coreutils.time import now_dt, dt_fromisostr
from op_analytics.coreutils.env.vault import env_get

from op_analytics.datasources.defillama.dataaccess import DefiLlama
from op_analytics.coreutils.partitioned.dailydatautils import most_recent_dates

log = structlog.get_logger()

API_KEY = env_get("DEFILLAMA_API_KEY")

YIELD_POOLS_ENDPOINT = "https://pro-api.llama.fi/{api_key}/yields/pools"
YIELD_POOL_CHART_ENDPOINT = "https://pro-api.llama.fi/{api_key}/yields/chart/{pool}"

YIELD_TABLE_LAST_N_DAYS = 120


@dataclass
class DefillamaYieldPools:
    """Metadata and yield data for all pools.

    This is the result we obtain after fetching from the API and extracting the data
    that we need to ingest.
    """

    yield_pools_df: pl.DataFrame


def execute_pull():
    result = pull_yield_pools_data()
    return {"yield_pools_df": dt_summary(result.yield_pools_df)}


def pull_yield_pools_data(pull_pools: list[str] | None = None) -> pl.DataFrame:
    """
    Pulls and processes yield pool data from DeFiLlama.

    Args:
        pull_pools: list of pool IDs to process. Defaults to None (process all).

    Returns:
        A polars DataFrame containing joined pool and historical yield data.
    """
    session = new_session()

    # Get all pools data
    pools_data = get_data(session, YIELD_POOLS_ENDPOINT.format(api_key=API_KEY))
    pools_df = extract_pools_data(pools_data["data"])

    # Write pools metadata
    DefiLlama.YIELD_POOLS_METADATA.write(
        dataframe=pools_df.with_columns(dt=pl.lit(now_dt())),
        sort_by=["pool"],
    )

    # Get pool IDs to process
    pool_ids = pools_df["pool"].to_list() if pull_pools is None else pull_pools

    # Call the API endpoint for each pool in parallel
    urls = {pool: YIELD_POOL_CHART_ENDPOINT.format(api_key=API_KEY, pool=pool) for pool in pool_ids}
    historical_yield_data = run_concurrently(lambda x: get_data(session, x), urls, max_workers=4)

    # Extract historical yield data
    historical_yield_df = extract_historical_yield_data(historical_yield_data)

    # Merge historical data with pool metadata
    yield_pools_df = historical_yield_df.join(pools_df, on="pool", how="left")

    # Write yield data
    DefiLlama.YIELD_POOLS_HISTORICAL.write(
        dataframe=most_recent_dates(
            yield_pools_df, n_dates=YIELD_TABLE_LAST_N_DAYS, date_column="dt"
        ),
        sort_by=["dt", "chain", "protocol_slug", "pool"],
    )

    return DefillamaYieldPools(yield_pools_df=yield_pools_df)


def extract_pools_data(pools_data: list) -> pl.DataFrame:
    """Extract pools data and transform into dataframe"""
    records = [
        {
            "pool": pool["pool"],
            "protocol_slug": pool["project"],
            "chain": pool["chain"],
            "symbol": pool["symbol"],
            "underlying_tokens": pool.get("underlyingTokens", []),
            "reward_tokens": pool.get("rewardTokens", []),
            "il_risk": pool["ilRisk"],
            "is_stablecoin": pool["stablecoin"],
            "exposure": pool["exposure"],
            # "tvl_usd": pool["tvlUsd"], # TODO: Add this back in if we use TVL filtering
        }
        for pool in pools_data
    ]

    return pl.DataFrame(records)


def extract_historical_yield_data(data: dict) -> pl.DataFrame:
    """Extract historical yield data and transform into dataframe"""
    records = [
        {
            "pool": pool_id,
            "dt": dt_fromisostr(entry["timestamp"]),
            "tvl_usd": float(entry.get("tvl", 0.0)),
            "apy": float(entry.get("apy", 0.0)),
            "apy_base": float(entry.get("apyBase") or 0.0),
            "apy_reward": float(entry.get("apyReward") or 0.0),
        }
        for pool_id, pool_data in data.items()
        for entry in pool_data["data"]
    ]

    return pl.DataFrame(records)
