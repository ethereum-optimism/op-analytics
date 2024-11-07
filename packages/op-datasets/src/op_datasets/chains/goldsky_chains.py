import polars as pl

from op_coreutils.clickhouse import run_goldsky_query
from op_coreutils.logger import structlog

from op_datasets.schemas import ONCHAIN_CURRENT_VERSION

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


def goldsky_mainnet_chains() -> list[str]:
    """List of mainnet chains ingested from Goldsky."""
    return sorted(goldsky_mainnet_chains_df()["oplabs_db_schema"].to_list())


def goldsky_testnet_chains() -> list[str]:
    """List of testnet chains ingested from Goldsky."""
    df = load_chain_metadata()
    return sorted(
        df.filter(pl.col("oplabs_testnet_db_schema").is_not_null())[
            "oplabs_testnet_db_schema"
        ].to_list()
    )


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
