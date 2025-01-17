from dataclasses import dataclass

import polars as pl
import re

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


def camel_to_snake(s: str) -> str:
    """
    Convert a camelCase or PascalCase string into snake_case,
    while handling certain acronyms as single blocks.

    Example:
      "someColumnName" -> "some_column_name"
      "logoURI"        -> "logo_uri"
    """

    for acronym in ["URI", "URL", "ID"]:
        s = s.replace(acronym, acronym.capitalize())

    return re.sub(r"(?<!^)(?=[A-Z])", "_", s).lower()


def process_metadata_pull(df) -> pl.DataFrame:
    """
    Unnest and cleanup metadata from Superchain token list.
    """
    df = (
        df.unnest("extensions")
        .rename(lambda c: camel_to_snake(c))
        .with_columns(pl.col("^.*address.*$").str.to_lowercase())
    )

    return df
