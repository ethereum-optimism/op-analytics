from dataclasses import dataclass

import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.misc import camel_to_snake, raise_for_schema_mismatch
from op_analytics.coreutils.partitioned.dailydata import DEFAULT_DT
from op_analytics.coreutils.partitioned.dailydatautils import dt_summary
from op_analytics.coreutils.request import get_data, new_session


from ..dataaccess import ChainsMeta

log = structlog.get_logger()

URL_BASE = "https://raw.githubusercontent.com/ethereum-optimism/ethereum-optimism.github.io/refs/heads/master/"
SUPERCHAIN_TOKEN_LIST = "optimism.tokenlist.json"


SUPERCHAIN_TOKEN_LIST_SCHEMA = pl.Schema(
    {
        "chain_id": pl.Int64,
        "address": pl.String,
        "name": pl.String,
        "symbol": pl.String,
        "decimals": pl.Int64,
        "logo_uri": pl.String,
        "optimism_bridge_address": pl.String,
        "mode_bridge_address": pl.String,
        "base_bridge_address": pl.String,
        "unichain_bridge_address": pl.String,  # added on 2025/02/11
        "op_list_id": pl.String,
        "op_token_id": pl.String,
        "dt": pl.String,
    }
)


@dataclass
class SuperchainTokenList:
    """Superchain token list pull from ethereum-optimism github repo."""

    token_list_df: pl.DataFrame


def execute_pull():
    result = pull_superchain_token_list()
    return {
        "token_list_df": dt_summary(result.token_list_df),
    }


def pull_superchain_token_list() -> SuperchainTokenList:
    """Pull data from ethereum-optimism github repo."""
    session = new_session()

    token_list_raw_data = get_data(session, f"{URL_BASE}{SUPERCHAIN_TOKEN_LIST}")

    # The schema is automatically inferred from the raw data.
    token_list_raw_df = pl.DataFrame(token_list_raw_data["tokens"])

    # Flatten the schema and convert to snake case.
    token_list_df = process_metadata_pull(token_list_raw_df).with_columns(dt=pl.lit(DEFAULT_DT))

    # Check the final schema is as expected. If something changes upstream the
    # exception will warn us.
    raise_for_schema_mismatch(
        actual_schema=token_list_df.schema,
        expected_schema=SUPERCHAIN_TOKEN_LIST_SCHEMA,
    )

    ChainsMeta.SUPERCHAIN_TOKEN_LIST.write(
        dataframe=token_list_df,
        sort_by=["address"],
    )

    return SuperchainTokenList(token_list_df=token_list_df)


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
