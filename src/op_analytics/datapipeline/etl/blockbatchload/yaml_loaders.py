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
    # Strategy 1: Try repo_path() first (works in most environments)
    try:
        from op_analytics.coreutils.path import repo_path

        repo_root = repo_path("src/op_analytics/configs")
        if repo_root is not None:
            config_path = Path(repo_root) / config_filename
            if config_path.exists():
                return config_path
    except Exception:
        pass

    # Strategy 2: Walk up from current file location (fallback)
    current_dir = Path(__file__).parent

    # Navigate up to find the repository root (where uv.lock is)
    while current_dir != current_dir.parent:
        if (current_dir / "uv.lock").exists():
            break
        current_dir = current_dir.parent
    else:
        # Strategy 3: Try common container paths
        container_paths = [
            Path("/app/src/op_analytics/configs"),
            Path("/workspace/src/op_analytics/configs"),
            Path("/code/src/op_analytics/configs"),
            Path("/usr/local/lib/python3.12/site-packages/op_analytics/configs"),
        ]

        for container_path in container_paths:
            config_path = container_path / config_filename
            if config_path.exists():
                return config_path

        # Strategy 4: Try relative to the current file's location
        # Navigate from the current file location to find the configs directory
        current_file_dir = Path(__file__).parent
        possible_config_paths = [
            current_file_dir / ".." / ".." / ".." / ".." / "configs" / config_filename,
            current_file_dir / ".." / ".." / ".." / "configs" / config_filename,
            current_file_dir / ".." / ".." / "configs" / config_filename,
        ]

        for config_path in possible_config_paths:
            if config_path.resolve().exists():
                return config_path.resolve()

        # Strategy 5: Try to find the config file anywhere in the package
        import op_analytics

        op_analytics_root = Path(op_analytics.__file__).parent
        config_path = op_analytics_root / "configs" / config_filename
        if config_path.exists():
            return config_path

        raise FileNotFoundError(
            f"Could not find config file '{config_filename}' in any expected location"
        )

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
