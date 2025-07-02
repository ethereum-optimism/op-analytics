import yaml
import polars as pl
from pathlib import Path

from op_analytics.coreutils.logger import structlog
from op_analytics.configs.dataaccess import RevshareConfig

log = structlog.get_logger()


def _get_config_path(config_filename: str) -> Path:
    """Get the path to a config file by navigating from the current file's location.

    Note: We originally tried using repo_path() from op_analytics.coreutils.path, but it
    was returning None in the Dagster execution environment (likely Kubernetes/Docker)
    because the working directory was not the repository root. This custom implementation
    is more robust as it doesn't rely on the working directory and provides better error
    messages when files can't be found.
    """
    # Start from the current file's directory
    current_dir = Path(__file__).parent

    # Navigate up to find the repository root (where uv.lock is)
    while current_dir != current_dir.parent:
        if (current_dir / "uv.lock").exists():
            break
        current_dir = current_dir.parent
    else:
        raise FileNotFoundError("Could not find repository root (uv.lock not found)")

    # Navigate to the config file
    config_path = current_dir / "src" / "op_analytics" / "configs" / config_filename
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    return config_path


def load_revshare_from_addresses_to_clickhouse():
    """Load revshare from addresses YAML into ClickHouse table."""

    # Read YAML config
    config_path = _get_config_path("revshare_from_addresses.yaml")
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
    config_path = _get_config_path("revshare_to_addresses.yaml")
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
