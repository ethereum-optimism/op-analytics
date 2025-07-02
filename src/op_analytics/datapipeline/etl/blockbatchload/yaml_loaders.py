import yaml
import polars as pl
from pathlib import Path
from op_analytics.coreutils.path import repo_path

from op_analytics.coreutils.logger import structlog
from op_analytics.configs.dataaccess import RevshareConfig

log = structlog.get_logger()


def load_revshare_from_addresses_to_clickhouse():
    """Load revshare from addresses YAML into ClickHouse table."""

    # Read YAML config
    config_path = Path(repo_path("src/op_analytics/configs/revshare_from_addresses.yaml"))
    with open(config_path) as f:
        from_config = yaml.safe_load(f)

    # Prepare data for insertion
    rows = []
    for chain, conf in from_config.items():
        for addr in conf["addresses"]:
            row = {
                "chain": chain,
                "address": str(addr).lower(),
                "tokens": [str(token).lower() for token in conf["tokens"]],
                "expected_chains": conf["expected_chains"],
                "end_date": conf.get("end_date"),
                "chain_id": conf.get("chain_id"),
            }
            rows.append(row)
            # print(f"DEBUG - From addresses row: {row}")

    # Convert to Polars DataFrame and write using ClickHouseDataset
    df = pl.DataFrame(rows)
    RevshareConfig.REVSHARE_FROM_ADDRESSES.write(df)

    log.info(f"Loaded {len(rows)} revshare_from_addresses records")


def load_revshare_to_addresses_to_clickhouse():
    """Load revshare to addresses YAML into ClickHouse table."""

    # Read YAML config
    config_path = Path(repo_path("src/op_analytics/configs/revshare_to_addresses.yaml"))
    with open(config_path) as f:
        to_config = yaml.safe_load(f)

    # Prepare data for insertion
    rows = []
    for addr, conf in to_config.items():
        row = {
            "address": str(addr).lower(),
            "description": conf["description"],
            "end_date": conf.get("end_date"),
            "expected_chains": conf["expected_chains"],
        }
        rows.append(row)
        # print(f"DEBUG - To addresses row: {row}")

    # Convert to Polars DataFrame and write using ClickHouseDataset
    df = pl.DataFrame(rows)
    RevshareConfig.REVSHARE_TO_ADDRESSES.write(df)

    log.info(f"Loaded {len(rows)} revshare_to_addresses records")
