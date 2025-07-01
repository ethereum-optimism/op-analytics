import yaml
import polars as pl
from pathlib import Path

from op_analytics.coreutils.clickhouse.ddl import read_ddl
from op_analytics.coreutils.clickhouse.oplabs import run_statememt_oplabs, insert_oplabs
from op_analytics.coreutils.logger import structlog

log = structlog.get_logger()


def load_revshare_from_addresses_to_clickhouse():
    """Load revshare from addresses YAML into ClickHouse table."""

    # Read YAML config
    config_path = Path(__file__).parents[2] / "configs" / "revshare_from_addresses.yaml"
    with open(config_path) as f:
        from_config = yaml.safe_load(f)

    # Prepare data for insertion
    rows = []
    for chain, conf in from_config.items():
        for addr in conf["addresses"]:
            rows.append(
                {
                    "chain": chain,
                    "address": addr.lower(),
                    "tokens": conf["tokens"],
                    "expected_chains": conf["expected_chains"],
                    "end_date": conf.get("end_date"),
                    "chain_id": conf.get("chain_id"),
                }
            )

    # Create table using DDL file
    ddl_path = (
        Path(__file__).parent / "ddl" / "revshare_transfers" / "revshare_from_addresses__CREATE.sql"
    )
    if ddl_path.exists():
        create_ddl = read_ddl(str(ddl_path))
        run_statememt_oplabs(create_ddl)
        log.info("Created revshare_from_addresses table from DDL")
    else:
        # Fallback to inline DDL if file doesn't exist
        create_ddl = """
        CREATE TABLE IF NOT EXISTS revshare_from_addresses (
            chain String,
            address String,
            tokens Array(String),
            expected_chains Array(String),
            end_date Nullable(String),
            chain_id Nullable(Int32)
        ) ENGINE = MergeTree()
        ORDER BY (chain, address)
        """
        run_statememt_oplabs(create_ddl)
        log.info("Created revshare_from_addresses table with inline DDL")

    # Truncate and insert data
    run_statememt_oplabs("TRUNCATE TABLE revshare_from_addresses")

    # Convert to Polars DataFrame and insert
    df = pl.DataFrame(rows)
    insert_oplabs(
        database="",  # Use default database
        table="revshare_from_addresses",
        df_arrow=df.to_arrow(),
    )

    log.info(f"Loaded {len(rows)} revshare_from_addresses records")


def load_revshare_to_addresses_to_clickhouse():
    """Load revshare to addresses YAML into ClickHouse table."""

    # Read YAML config
    config_path = Path(__file__).parents[2] / "configs" / "revshare_to_addresses.yaml"
    with open(config_path) as f:
        to_config = yaml.safe_load(f)

    # Prepare data for insertion
    rows = []
    for addr, conf in to_config.items():
        rows.append(
            {
                "address": addr.lower(),
                "description": conf["description"],
                "end_date": conf.get("end_date"),
                "expected_chains": conf["expected_chains"],
            }
        )

    # Create table using DDL file
    ddl_path = (
        Path(__file__).parent / "ddl" / "revshare_transfers" / "revshare_to_addresses__CREATE.sql"
    )
    if ddl_path.exists():
        create_ddl = read_ddl(str(ddl_path))
        run_statememt_oplabs(create_ddl)
        log.info("Created revshare_to_addresses table from DDL")
    else:
        # Fallback to inline DDL if file doesn't exist
        create_ddl = """
        CREATE TABLE IF NOT EXISTS revshare_to_addresses (
            address String,
            description String,
            end_date Nullable(String),
            expected_chains Array(String)
        ) ENGINE = MergeTree()
        ORDER BY address
        """
        run_statememt_oplabs(create_ddl)
        log.info("Created revshare_to_addresses table with inline DDL")

    # Truncate and insert data
    run_statememt_oplabs("TRUNCATE TABLE revshare_to_addresses")

    # Convert to Polars DataFrame and insert
    df = pl.DataFrame(rows)
    insert_oplabs(
        database="",  # Use default database
        table="revshare_to_addresses",
        df_arrow=df.to_arrow(),
    )

    log.info(f"Loaded {len(rows)} revshare_to_addresses records")
