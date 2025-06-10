from dataclasses import dataclass

import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.misc import raise_for_schema_mismatch, camel_to_snake
from op_analytics.coreutils.partitioned.dailydata import DEFAULT_DT
from op_analytics.coreutils.partitioned.dailydatautils import dt_summary
from op_analytics.coreutils.request import get_data, new_session

from ..dataaccess import ChainsMeta

log = structlog.get_logger()

URL_BASE = (
    "https://raw.githubusercontent.com/ethereum-optimism/superchain-registry/refs/heads/main/"
)
SUPERCHAIN_CHAIN_LIST = "chainList.json"

SUPERCHAIN_CHAIN_LIST_SCHEMA = pl.Schema(
    [
        ("name", pl.String),
        ("identifier", pl.String),
        ("chain_id", pl.Int32),
        ("rpc", pl.List(pl.String)),
        ("explorers", pl.List(pl.String)),
        ("superchain_level", pl.Int32),
        ("governed_by_optimism", pl.Boolean),
        ("data_availability_type", pl.String),
        ("parent_type", pl.String),
        ("parent_chain", pl.String),
        ("gas_paying_token", pl.String),
        ("fault_proofs_status", pl.String),
    ]
)


@dataclass
class SuperchainChainList:
    """Superchain chain list pull from ethereum-optimism github repo."""

    chain_list_df: pl.DataFrame


def execute_pull():
    result = pull_superchain_chain_list()
    return {
        "chain_list_df": dt_summary(result.chain_list_df),
    }


def pull_superchain_chain_list() -> SuperchainChainList:
    """Pull data from ethereum-optimism github repo."""
    session = new_session()

    chain_list_raw_data = get_data(session, f"{URL_BASE}{SUPERCHAIN_CHAIN_LIST}")

    # Convert the list of records to DataFrame
    chain_list_raw_df = pl.DataFrame(chain_list_raw_data)

    # Flatten the schema and convert to snake case.
    chain_list_df = process_metadata_pull(chain_list_raw_df)

    # Reorder columns to match the expected schema
    expected_column_order = list(SUPERCHAIN_CHAIN_LIST_SCHEMA.keys())
    chain_list_df = chain_list_df.select(expected_column_order)

    # Check the final schema is as expected. If something changes upstream the
    # exception will warn us.
    raise_for_schema_mismatch(
        actual_schema=chain_list_df.schema,
        expected_schema=SUPERCHAIN_CHAIN_LIST_SCHEMA,
    )

    # Convert gas_paying_token to lowercase, handling null values
    chain_list_df = chain_list_df.with_columns(pl.col("gas_paying_token").str.to_lowercase())

    # Add dt column after schema validation
    chain_list_df = chain_list_df.with_columns(dt=pl.lit(DEFAULT_DT))

    ChainsMeta.SUPERCHAIN_CHAIN_LIST.write(
        dataframe=chain_list_df,
        sort_by=["chain_id"],
    )

    return SuperchainChainList(chain_list_df=chain_list_df)


def process_metadata_pull(df) -> pl.DataFrame:
    """
    Cleanup metadata from Superchain chain list.
    """
    # Flatten nested objects and rename columns
    df = (
        df.with_columns(
            pl.col("parent").struct.field("type").alias("parent_type"),
            pl.col("parent").struct.field("chain").alias("parent_chain"),
            pl.col("faultProofs").struct.field("status").alias("fault_proofs_status"),
        )
        .drop(["parent", "faultProofs"])
        .rename(lambda c: camel_to_snake(c))
        .with_columns(pl.col("chain_id").cast(pl.Int32), pl.col("superchain_level").cast(pl.Int32))
    )

    return df
