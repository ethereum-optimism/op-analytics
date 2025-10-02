"""Superchain registry configuration data from GitHub."""

from dataclasses import dataclass
from typing import Dict, Any

import polars as pl
import requests
import tomli

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.misc import raise_for_schema_mismatch
from op_analytics.coreutils.time import now_date
from op_analytics.coreutils.partitioned.dailydatautils import dt_summary
from op_analytics.coreutils.request import new_session

from ..dataaccess import ChainsMeta

log = structlog.get_logger()

# GitHub superchain registry base URL
GITHUB_REGISTRY_BASE = "https://raw.githubusercontent.com/ethereum-optimism/superchain-registry/main/superchain/configs/mainnet/"

# Schema for superchain registry data
SUPERCHAIN_REGISTRY_SCHEMA = pl.Schema(
    {
        "chain_name": pl.String,
        "dt": pl.String,
        # Basic chain info
        "name": pl.String,
        "chain_id": pl.Int64,
        "public_rpc": pl.String,
        "sequencer_rpc": pl.String,
        "explorer": pl.String,
        "superchain_level": pl.Int64,
        "governed_by_optimism": pl.Boolean,
        "superchain_time": pl.Int64,
        "data_availability_type": pl.String,
        "batch_inbox_addr": pl.String,
        "block_time": pl.Int64,
        "seq_window_size": pl.Int64,
        "max_sequencer_drift": pl.Int64,
        # Hardforks section
        "hardforks_canyon_time": pl.Int64,
        "hardforks_delta_time": pl.Int64,
        "hardforks_ecotone_time": pl.Int64,
        "hardforks_fjord_time": pl.Int64,
        "hardforks_granite_time": pl.Int64,
        "hardforks_holocene_time": pl.Int64,
        "hardforks_isthmus_time": pl.Int64,
        # Optimism section fields
        "optimism_eip1559_elasticity": pl.Int64,
        "optimism_eip1559_denominator": pl.Int64,
        "optimism_eip1559_denominator_canyon": pl.Int64,
        # Genesis section
        "genesis_l2_time": pl.Int64,
        "genesis_l1_hash": pl.String,
        "genesis_l1_number": pl.Int64,
        "genesis_l2_hash": pl.String,
        "genesis_l2_number": pl.Int64,
        "genesis_system_config_gasLimit": pl.Int64,
        "genesis_system_config_batcherAddress": pl.String,
        "genesis_system_config_scalar": pl.String,
        "genesis_system_config_overhead": pl.String,
        # Roles section
        "roles_ProxyAdminOwner": pl.String,
        # Addresses section
        "addresses_SystemConfigProxy": pl.String,
        "addresses_OptimismPortalProxy": pl.String,
        "addresses_L1StandardBridgeProxy": pl.String,
        "addresses_DisputeGameFactoryProxy": pl.String,
        # Alt DA section
        "alt_da_da_challenge_window": pl.Int64,
        "alt_da_da_resolve_window": pl.Int64,
        "alt_da_da_commitment_type": pl.String,
        "alt_da_da_challenge_contract_address": pl.String,
        # Additional fields
        "gas_paying_token": pl.String,
        "superchain_config_addr": pl.String,
        "protocol_versions_addr": pl.String,
        # L1 section
        "l1_public_rpc": pl.String,
        "l1_chain_id": pl.Int64,
        "l1_explorer": pl.String,
    }
)


@dataclass
class SuperchainRegistry:
    """Superchain registry configuration data from GitHub."""

    registry_df: pl.DataFrame


def execute_pull():
    """Execute the superchain registry data pull."""
    result = pull_superchain_registry()
    return {
        "registry_df": dt_summary(result.registry_df),
    }


def pull_superchain_registry() -> SuperchainRegistry:
    """Pull all TOML configuration files from GitHub superchain registry."""
    session = new_session()

    # Get list of available chains from the registry
    chains = get_available_chains()
    log.info(f"Found {len(chains)} chains in superchain registry")

    # Fetch all TOML configs
    all_configs = []
    successful_fetches = 0

    for i, chain in enumerate(chains, 1):
        log.info(f"Fetching config for {chain} ({i}/{len(chains)})")
        config_data = fetch_chain_config(session, chain)
        if config_data:
            all_configs.append(config_data)
            successful_fetches += 1
            log.info(f"Successfully fetched config for {chain}")
        else:
            raise ValueError(f"No config data found for {chain}")

    log.info(f"Fetch summary: {successful_fetches} successful out of {len(chains)} chains")

    if not all_configs:
        raise ValueError("No chain configurations were successfully fetched")

    # Convert to DataFrame
    registry_df = pl.DataFrame(all_configs)
    log.info(
        f"Created DataFrame with {len(registry_df)} rows and {len(registry_df.columns)} columns"
    )
    log.info(f"Final columns: {registry_df.columns}")

    # Add dt column with current date
    current_date = now_date().strftime("%Y-%m-%d")
    registry_df = registry_df.with_columns(dt=pl.lit(current_date))

    # Create dynamic schema based on actual data
    dynamic_schema = pl.Schema({col: registry_df[col].dtype for col in registry_df.columns})

    # Validate schema
    log.info("Validating dynamic schema...")
    raise_for_schema_mismatch(
        actual_schema=registry_df.schema,
        expected_schema=dynamic_schema,
    )
    log.info("Schema validation passed")

    # Write to storage
    ChainsMeta.SUPERCHAIN_CONFIG.write(
        dataframe=registry_df,
        sort_by=["chain_name"],
    )

    log.info(f"Stored {len(registry_df)} superchain config records")

    return SuperchainRegistry(registry_df=registry_df)


def get_available_chains() -> list[str]:
    """Get list of available chains from the GitHub registry dynamically."""
    session = new_session()

    try:
        # Use GitHub API to get the contents of the mainnet configs directory
        api_url = "https://api.github.com/repos/ethereum-optimism/superchain-registry/contents/superchain/configs/mainnet"

        response = session.get(api_url)
        response.raise_for_status()

        files = response.json()

        # Extract chain names from TOML files
        chains = []
        for file_info in files:
            if file_info.get("type") == "file" and file_info.get("name", "").endswith(".toml"):
                # Remove .toml extension to get chain name
                chain_name = file_info["name"][:-5]  # Remove last 5 characters (.toml)
                chains.append(chain_name)

        chains.sort()  # Sort for consistent ordering
        log.info(f"Discovered {len(chains)} chains dynamically from GitHub registry")

        return chains

    except Exception as e:
        log.error(f"Failed to fetch chains from GitHub API: {e}")
        raise ValueError(f"Cannot fetch chain list from GitHub API: {e}")


def fetch_chain_config(session: requests.Session, chain: str) -> Dict[str, Any] | None:
    """Fetch TOML configuration for a specific chain."""
    url = f"{GITHUB_REGISTRY_BASE}{chain}.toml"

    try:
        response = session.get(url)
        response.raise_for_status()

        # Parse TOML content
        config = tomli.loads(response.text)

        # Start with basic chain metadata
        chain_data = {
            "chain_name": chain,
            "name": config.get("name", chain),
            "chain_id": config.get("chain_id"),
            "public_rpc": config.get("public_rpc"),
            "sequencer_rpc": config.get("sequencer_rpc"),
            "explorer": config.get("explorer"),
            "superchain_level": config.get("superchain_level"),
            "governed_by_optimism": config.get("governed_by_optimism"),
            "superchain_time": config.get("superchain_time"),
            "data_availability_type": config.get("data_availability_type"),
            "batch_inbox_addr": config.get("batch_inbox_addr"),
            "block_time": config.get("block_time"),
            "seq_window_size": config.get("seq_window_size"),
            "max_sequencer_drift": config.get("max_sequencer_drift"),
            "identifier": config.get("identifier", f"mainnet/{chain}"),
        }

        # Extract hardforks section
        hardforks = config.get("hardforks", {})
        if hardforks:
            chain_data.update(
                {
                    "hardforks_canyon_time": hardforks.get("canyon_time"),
                    "hardforks_delta_time": hardforks.get("delta_time"),
                    "hardforks_ecotone_time": hardforks.get("ecotone_time"),
                    "hardforks_fjord_time": hardforks.get("fjord_time"),
                    "hardforks_granite_time": hardforks.get("granite_time"),
                    "hardforks_holocene_time": hardforks.get("holocene_time"),
                    "hardforks_isthmus_time": hardforks.get("isthmus_time"),
                }
            )

        # Extract optimism section
        optimism_config = config.get("optimism", {})
        if optimism_config:
            chain_data.update(
                {
                    "optimism_eip1559_elasticity": optimism_config.get("eip1559_elasticity"),
                    "optimism_eip1559_denominator": optimism_config.get("eip1559_denominator"),
                    "optimism_eip1559_denominator_canyon": optimism_config.get(
                        "eip1559_denominator_canyon"
                    ),
                }
            )

        # Extract genesis section
        genesis = config.get("genesis", {})
        if genesis:
            chain_data.update(
                {
                    "genesis_l2_time": genesis.get("l2_time"),
                }
            )

            # Genesis L1
            genesis_l1 = genesis.get("l1", {})
            if genesis_l1:
                chain_data.update(
                    {
                        "genesis_l1_hash": genesis_l1.get("hash"),
                        "genesis_l1_number": genesis_l1.get("number"),
                    }
                )

            # Genesis L2
            genesis_l2 = genesis.get("l2", {})
            if genesis_l2:
                chain_data.update(
                    {
                        "genesis_l2_hash": genesis_l2.get("hash"),
                        "genesis_l2_number": genesis_l2.get("number"),
                    }
                )

            # Genesis system config
            genesis_system_config = genesis.get("system_config", {})
            if genesis_system_config:
                chain_data.update(
                    {
                        "genesis_system_config_batcherAddress": genesis_system_config.get(
                            "batcherAddress"
                        ),
                        "genesis_system_config_overhead": str(
                            genesis_system_config.get("overhead", "")
                        ),
                        "genesis_system_config_scalar": str(
                            genesis_system_config.get("scalar", "")
                        ),
                        "genesis_system_config_gasLimit": genesis_system_config.get("gasLimit"),
                    }
                )

        # Extract roles section
        roles = config.get("roles", {})
        if roles:
            chain_data.update(
                {
                    "roles_ProxyAdminOwner": roles.get("ProxyAdminOwner"),
                }
            )

        # Extract addresses section
        addresses = config.get("addresses", {})
        if addresses:
            chain_data.update(
                {
                    "addresses_L1StandardBridgeProxy": addresses.get("L1StandardBridgeProxy"),
                    "addresses_OptimismPortalProxy": addresses.get("OptimismPortalProxy"),
                    "addresses_SystemConfigProxy": addresses.get("SystemConfigProxy"),
                    "addresses_DisputeGameFactoryProxy": addresses.get("DisputeGameFactoryProxy"),
                }
            )

        # Extract alt_da section (for chains with alternative DA)
        alt_da = config.get("alt_da", {})
        if alt_da:
            chain_data.update(
                {
                    "alt_da_da_challenge_window": alt_da.get("da_challenge_window"),
                    "alt_da_da_resolve_window": alt_da.get("da_resolve_window"),
                    "alt_da_da_commitment_type": alt_da.get("da_commitment_type"),
                    "alt_da_da_challenge_contract_address": alt_da.get(
                        "da_challenge_contract_address"
                    ),
                }
            )

        # Extract additional fields
        chain_data.update(
            {
                "gas_paying_token": config.get("gas_paying_token"),
                "superchain_config_addr": config.get("superchain_config_addr"),
                "protocol_versions_addr": config.get("protocol_versions_addr"),
            }
        )

        # Extract l1 section (for specific chains)
        l1_config = config.get("l1", {})
        if l1_config:
            chain_data.update(
                {
                    "l1_public_rpc": l1_config.get("public_rpc"),
                    "l1_chain_id": l1_config.get("chain_id"),
                    "l1_explorer": l1_config.get("explorer"),
                }
            )

        return chain_data

    except requests.exceptions.RequestException as e:
        log.error(f"HTTP error fetching config for {chain}: {e}")
        return None
    except tomli.TOMLDecodeError as e:
        log.error(f"TOML parsing error for {chain}: {e}")
        return None
    except Exception as e:
        log.error(f"Unexpected error fetching config for {chain}: {e}")
        return None
