from dataclasses import dataclass

import polars as pl

from op_analytics.coreutils.bigquery.write import most_recent_dates
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.request import get_data, new_session
from op_analytics.coreutils.time import now_dt

from .dataaccess import EthereumOptimism

log = structlog.get_logger()

URL_BASE = "https://raw.githubusercontent.com/ethereum-optimism/ethereum-optimism.github.io/refs/heads/master/"
SUPERCHAIN_TOKEN_LIST = "optimism.tokenlist.json"


@dataclass
class SuperchainTokenList:
    """Superchain token list pull from ethereum-optimism github repo."""

    token_list_df: pl.DataFrame


def execute_pull():
    result = pull_superchain_token_list()
    return {
        "token_list_df": len(result.token_list_df),
    }


def pull_superchain_token_list() -> SuperchainTokenList:
    """Pull data from ethereum-optimism github repo."""
    session = new_session()

    token_list_raw_data = get_data(session, f"{URL_BASE}{SUPERCHAIN_TOKEN_LIST}")
    token_list_df = pl.DataFrame(token_list_raw_data["tokens"])

    token_list_df = process_metadata_pull(token_list_df)

    EthereumOptimism.SUPERCHAIN_TOKEN_LIST.write(
        dataframe=token_list_df.with_columns(dt=pl.lit(now_dt())),
        sort_by=["address"],
    )

    return SuperchainTokenList(
        token_list_df=token_list_df,
    )


def process_metadata_pull(df) -> pl.DataFrame:
    """
    Unnest and cleanup metadata from Superchain token list.
    """
    df = df.unnest("extensions")

    df = df.with_columns(
        pl.col(
            ["address", "optimismBridgeAddress", "baseBridgeAddress", "modeBridgeAddress"]
        ).str.to_lowercase()
    ).rename(
        {
            "chainId": "chain_id",
            "logoURI": "logo_uri",
            "optimismBridgeAddress": "optimism_bridge_address",
            "baseBridgeAddress": "base_bridge_address",
            "modeBridgeAddress": "mode_bridge_address",
            "opListId": "op_list_id",
            "opTokenId": "op_token_id",
        }
    )

    return df
