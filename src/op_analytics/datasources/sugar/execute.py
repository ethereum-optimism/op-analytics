from sugar.chains import BaseChain, OPChain
import polars as pl
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydatautils import dt_summary
from op_analytics.dagster.assets.sugar import Sugar
from op_analytics.datasources.sugar.tokens import fetch_tokens_for_chain
from op_analytics.datasources.sugar.pools import fetch_pools_for_chain
from op_analytics.datasources.sugar.prices import fetch_prices_for_chain

log = structlog.get_logger()

CHAIN_LIST = [OPChain, BaseChain]


def _collect_data() -> dict[str, list[pl.DataFrame]]:
    """
    Collects tokens, pools, and prices data from each chain in CHAIN_LIST,
    by leveraging sugarwrapper.py calls indirectly via sugar.tokens, sugar.pools, sugar.prices.
    Returns lists of DataFrames for each data type.
    """
    all_data = {"tokens": [], "pools": [], "prices": []}

    for chain_cls in CHAIN_LIST:
        chain_name = chain_cls.__name__
        log.info(f"Fetching Sugar data for chain={chain_name}")

        chain_tokens_df = fetch_tokens_for_chain(chain_cls)
        chain_pools_df = fetch_pools_for_chain(chain_cls)
        chain_prices_df = fetch_prices_for_chain(chain_cls)

        all_data["tokens"].append(chain_tokens_df)
        all_data["pools"].append(chain_pools_df)
        all_data["prices"].append(chain_prices_df)

    return all_data


def execute_pull() -> dict[str, dict]:
    """
    Main Sugar ingestion entrypoint. Fetches tokens, pools, and prices
    for each chain in CHAIN_LIST, then writes them to the Sugar dataset.
    Returns a summary of counts for logging.
    """
    collected = _collect_data()
    summary = {}

    Sugar.write_dataset(collected["tokens"], Sugar.TOKENS)
    summary["tokens_df"] = dt_summary(collected["tokens"])

    Sugar.write_dataset(collected["pools"], Sugar.POOLS)
    summary["pools_df"] = dt_summary(collected["pools"])

    Sugar.write_dataset(collected["prices"], Sugar.PRICES)
    summary["prices_df"] = dt_summary(collected["prices"])

    log.info("Sugar ingestion completed", summary=summary)
    return summary
