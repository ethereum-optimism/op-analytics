from enum import Enum
from functools import cache

import polars as pl

from op_analytics.coreutils.clickhouse.goldsky import run_query_goldsky
from op_analytics.coreutils.logger import structlog

from op_analytics.datapipeline.schemas import ONCHAIN_CURRENT_VERSION

from .load import load_chain_metadata

log = structlog.get_logger()


def goldsky_mainnet_chains_df() -> pl.DataFrame:
    """Dataframe with mainnet chains ingested from Goldsky."""
    df = load_chain_metadata()

    return (
        df.filter(pl.col("oplabs_db_schema").is_not_null())
        # (pedro - 2025/02/11) Exclude chains temporarily. Need to set up
        # batch configuration and get back to this soon.
        .filter(
            ~pl.col("oplabs_db_schema").is_in(
                [
                    "ethernity",
                    "superseed",
                    "snaxchain",
                ]
            )
        )
        .select(
            "chain_name",
            "display_name",
            "mainnet_chain_id",
            "oplabs_db_schema",
        )
        .sort("chain_name")
    )


@cache
def goldsky_mainnet_chains() -> list[str]:
    """List of mainnet chains ingested from Goldsky."""
    # (pedrod - 2025/03/04) Ran into gsheets rate limiting issues when backfilling
    # with many pods in parallel. So opted to hard-code the list.
    # return sorted(goldsky_mainnet_chains_df()["oplabs_db_schema"].to_list())
    return [
        "arenaz",
        "automata",
        "base",
        "bob",
        "celo",
        "cyber",
        # "ethereum", (ggarner - 2025/07/14) Exclude ethereum due to downstream dtype issues
        "fraxtal",
        "ham",
        "ink",
        # "kroma",  # (ggarner - 2025/07/07) Exclude kroma due to chain shutting down.
        "lisk",
        "lyra",
        "metal",
        "mint",
        "mode",
        "op",
        "orderly",
        "polynomial",
        "race",
        "redstone",
        "shape",
        "soneium",
        "swan",
        "swell",
        "unichain",
        "worldchain",
        # "xterio", # (pedro - 2025/05/10) Exclude xterio due to chain shutting down.
        "zora",
    ]


@cache
def goldsky_testnet_chains() -> list[str]:
    """List of testnet chains ingested from Goldsky."""
    # (pedrod - 2025/03/04) Ran into gsheets rate limiting issues when backfilling
    # with many pods in parallel. So opted to hard-code the list.
    # df = load_chain_metadata()
    # return sorted(
    #     df.filter(pl.col("oplabs_testnet_db_schema").is_not_null())[
    #         "oplabs_testnet_db_schema"
    #     ].to_list()
    # )
    return [
        "ink_sepolia",
        "op_sepolia",
        "unichain_sepolia",
    ]


class ChainNetwork(Enum):
    """The network of a chain."""

    MAINNET = 0
    TESTNET = 1


@cache
def determine_network(chain: str) -> ChainNetwork:
    if chain.endswith("_sepolia"):
        return ChainNetwork.TESTNET
    else:
        return ChainNetwork.MAINNET

    # NOTE: The implementation below relies on reading the chain metadata gsheet.
    #       For now it is simpler to use the "_sepolia" suffix to split testenets.
    # if chain in goldsky_mainnet_chains():
    #     return ChainNetwork.MAINNET
    # if chain in goldsky_testnet_chains():
    #     return ChainNetwork.TESTNET
    # raise ValueError(f"could not determine network for chain: {chain!r}")


def verify_goldsky_tables(chains: list[str]) -> None:
    """Run queries to esnure the required tables exist for the listed chains."""
    tables = []
    for chain in chains:
        for _, dataset in ONCHAIN_CURRENT_VERSION.items():
            tables.append(f"{chain}_{dataset.goldsky_table_suffix}")
    tables_filter = ",\n".join([f"'{t}'" for t in tables])

    query = f"""
    SELECT 
        name as table_name
    FROM system.tables
    WHERE name IN ({tables_filter})
    """
    results = run_query_goldsky(query)["table_name"].to_list()

    expected_tables = set(tables)

    missing_tables = expected_tables - set(results)

    if missing_tables:
        for name in sorted(missing_tables):
            log.error(f"ERROR: Table missing in Goldsky Clickhouse: {name!r}")
    else:
        log.info("SUCCESS: All expected tables are present in Goldsky Clickhouse")
        for name in sorted(expected_tables):
            log.info("    " + name)
