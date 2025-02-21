"""
Sugar data ingestion pipeline.

This module pulls Sugar protocol data from all chains and writes the tokens, pools,
and prices to partitioned datasets in ClickHouse or GCS (depending on the configuration).
"""

import polars as pl
from typing import Dict, Any, List

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydatautils import dt_summary
from op_analytics.datasources.sugar.dataaccess import SugarDataAccess
from op_analytics.datasources.sugar.chain_list import chain_list
from op_analytics.datasources.sugar.chains.chains import fetch_chain_data

log = structlog.get_logger()


async def _collect_data() -> Dict[str, List[Dict[str, Any]]]:
    """
    Collects tokens, pools, and prices data from each configured chain (OPChain, BaseChain).
    Returns:
        Dictionary containing three lists, keyed by "tokens", "pools", and "prices".
    """
    all_data = {"tokens": [], "pools": [], "prices": []}

    for chain_cls in chain_list:
        tokens, pools, prices = await fetch_chain_data(chain_cls)
        all_data["tokens"].extend(tokens)
        all_data["pools"].extend(pools)
        all_data["prices"].extend(prices)

    return all_data


def _write_data(
    data: List[Dict[str, Any]],
    dataset: SugarDataAccess,
    data_type: str,
) -> Dict[str, Any]:
    """
    Writes data to the dataset and returns a summary for logging.
    """
    df = pl.DataFrame(data)
    dataset.write(df)

    summary = {f"{data_type}_df": dt_summary(df)}
    log.info(f"Sugar {data_type} ingestion completed", summary=summary)
    return summary


async def execute_pull() -> Dict[str, Any]:
    """
    Main Sugar ingestion entrypoint.
    Fetches the data from all chains, writes to configured datasets,
    and returns a summary dictionary.
    """
    all_data = await _collect_data()

    summary: Dict[str, Any] = {}
    summary.update(_write_data(all_data["tokens"], SugarDataAccess.TOKENS, "tokens"))
    summary.update(_write_data(all_data["pools"], SugarDataAccess.POOLS, "pools"))
    summary.update(_write_data(all_data["prices"], SugarDataAccess.PRICES, "prices"))

    log.info("Sugar ingestion completed", summary=summary)
    return summary
