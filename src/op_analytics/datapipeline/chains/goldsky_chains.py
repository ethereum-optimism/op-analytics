from enum import Enum
from functools import cache

import polars as pl

from op_analytics.coreutils.clickhouse import run_goldsky_query
from op_analytics.coreutils.logger import structlog

from op_analytics.datapipeline.schemas import ONCHAIN_CURRENT_VERSION

from .load import load_chain_metadata

log = structlog.get_logger()


def goldsky_mainnet_chains_df() -> pl.DataFrame:
    """Dataframe with mainnet chains ingested from Goldsky."""
    df = load_chain_metadata()

    return (
        df.filter(pl.col("oplabs_db_schema").is_not_null())
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
    return sorted(goldsky_mainnet_chains_df()["oplabs_db_schema"].to_list())


@cache
def goldsky_testnet_chains() -> list[str]:
    """List of testnet chains ingested from Goldsky."""
    df = load_chain_metadata()
    return sorted(
        df.filter(pl.col("oplabs_testnet_db_schema").is_not_null())[
            "oplabs_testnet_db_schema"
        ].to_list()
    )


class ChainNetwork(Enum):
    """Supported storage locations for partitioned data."""

    MAINNET = 0
    TESTNET = 1


def determine_network(chains: list[str]) -> ChainNetwork:
    if set(chains).issubset(goldsky_mainnet_chains()):
        return ChainNetwork.MAINNET

    if set(chains).issubset(goldsky_testnet_chains()):
        return ChainNetwork.TESTNET

    raise ValueError("chain list contains both MAINNET and TESTNET chains.")


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
    results = run_goldsky_query(query)["table_name"].to_list()

    expected_tables = set(tables)

    missing_tables = expected_tables - set(results)

    if missing_tables:
        for name in sorted(missing_tables):
            log.error(f"ERROR: Table missing in Goldsky Clickhouse: {name!r}")
    else:
        log.info("SUCCESS: All expected tables are present in Goldsky Clickhouse")
        for name in sorted(expected_tables):
            log.info("    " + name)
