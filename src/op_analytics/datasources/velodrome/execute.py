import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydatautils import dt_summary
from op_analytics.datasources.velodrome.chain_list import SUGAR_CHAINS
from op_analytics.datasources.velodrome.pools import fetch_pools_for_chain
from op_analytics.datasources.velodrome.prices import fetch_prices_for_chain
from op_analytics.datasources.velodrome.tokens import fetch_tokens_for_chain
from .dataaccess import Velodrome

log = structlog.get_logger()


def _collect_data() -> dict[str, list[pl.DataFrame]]:
    """
    Gather tokens, pools, and prices DataFrames for each chain in SUGAR_CHAINS.
    Returns a dict mapping "tokens", "pools", and "prices" to lists of DataFrames.
    """
    all_data = {"tokens": [], "pools": [], "prices": []}

    for chain_name, chain_instance in SUGAR_CHAINS.items():
        log.info("Fetching Velodrome data through sugar-sdk", chain=chain_name)

        chain_cls = type(chain_instance)
        chain_tokens_df = fetch_tokens_for_chain(chain_cls)
        chain_pools_df = fetch_pools_for_chain(chain_cls)
        chain_prices_df = fetch_prices_for_chain(chain_cls)

        all_data["tokens"].append(chain_tokens_df)
        all_data["pools"].append(chain_pools_df)
        all_data["prices"].append(chain_prices_df)

    return all_data


def execute_pull() -> dict[str, dict]:
    """
    Main entry point for Velodrome data ingestion logic. Fetches tokens, pools, and prices for each chain,

    Returns:
        dict: Summary of the data ingestion process.
    """
    log.info("Starting Velodrome data ingestion")

    collected = _collect_data()
    summary = {}

    Velodrome.write_dataset(collected["tokens"], Velodrome.TOKENS)
    summary["tokens_df"] = dt_summary(collected["tokens"])
    log.info("Tokens data written successfully", count=len(collected["tokens"]))

    Velodrome.write_dataset(collected["pools"], Velodrome.POOLS)
    summary["pools_df"] = dt_summary(collected["pools"])
    log.info("Pools data written successfully", count=len(collected["pools"]))

    Velodrome.write_dataset(collected["prices"], Velodrome.PRICES)
    summary["prices_df"] = dt_summary(collected["prices"])
    log.info("Prices data written successfully", count=len(collected["prices"]))

    log.info("Velodrome ingestion completed", summary=summary)
    return summary
