import yaml
import clickhouse_connect
from pathlib import Path
import os


def _get_clickhouse_client(client=None):
    """Get ClickHouse client with environment variable support."""
    if client is None:
        host = os.getenv("OP_CLICKHOUSE_HOST", "localhost")
        user = os.getenv("OP_CLICKHOUSE_USER", "default")
        password = os.getenv("OP_CLICKHOUSE_PW", "")
        client = clickhouse_connect.get_client(host=host, username=user, password=password)
    return client


def load_revshare_from_addresses_to_clickhouse(client=None):
    client = _get_clickhouse_client(client)

    config_path = Path(__file__).parents[2] / "configs" / "revshare_from_addresses.yaml"
    with open(config_path) as f:
        from_config = yaml.safe_load(f)

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

    client.command("""
        CREATE TABLE IF NOT EXISTS revshare_from_addresses (
            chain String,
            address String,
            tokens Array(String),
            expected_chains Array(String),
            end_date Nullable(String),
            chain_id UInt32
        ) ENGINE = MergeTree()
        ORDER BY (chain, address)
    """)

    client.command("TRUNCATE TABLE revshare_from_addresses")
    client.insert(
        "revshare_from_addresses",
        rows,
        column_names=["chain", "address", "tokens", "expected_chains", "end_date", "chain_id"],
    )


def load_revshare_to_addresses_to_clickhouse(client=None):
    client = _get_clickhouse_client(client)

    config_path = Path(__file__).parents[2] / "configs" / "revshare_to_addresses.yaml"
    with open(config_path) as f:
        to_config = yaml.safe_load(f)

    rows = []
    for addr, conf in to_config.items():
        rows.append(
            {
                "address": addr.lower(),
                "description": conf["description"],
                "end_date": conf["end_date"],
                "expected_chains": conf["expected_chains"],
            }
        )

    client.command("""
        CREATE TABLE IF NOT EXISTS revshare_to_addresses (
            address String,
            description String,
            end_date Nullable(String),
            expected_chains Array(String)
        ) ENGINE = MergeTree()
        ORDER BY address
    """)

    client.command("TRUNCATE TABLE revshare_to_addresses")
    client.insert(
        "revshare_to_addresses",
        rows,
        column_names=["address", "description", "end_date", "expected_chains"],
    )
