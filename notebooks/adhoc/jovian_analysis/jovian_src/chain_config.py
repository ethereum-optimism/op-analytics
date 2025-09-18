"""
Chain configuration for Jovian multi-chain analysis.
Defines chain-specific parameters and configurations.
"""

from typing import Dict, Any, List
from pathlib import Path
from .constants import (
    CHAIN_DEFAULT_GAS_LIMIT
)

# Chain configurations
CHAIN_CONFIGS: Dict[str, Dict[str, Any]] = {
    "base": {
        "name": "Base",
        "display_name": "Base",
        "gas_limits_file": "base_gas_limits.csv",
        "default_gas_limit": CHAIN_DEFAULT_GAS_LIMIT,
        "color": "#0052FF",  # Base blue
        "priority": 1,
        "enabled": True,
        "identifier": "mainnet/base"
    },
    "op": {
        "name": "OP",
        "display_name": "OP Mainnet",
        "gas_limits_file": "op_gas_limits.csv",
        "default_gas_limit": CHAIN_DEFAULT_GAS_LIMIT,
        "color": "#FF0420",  # OP red
        "priority": 2,
        "enabled": True,
        "identifier": "mainnet/op"
    },
    "mode": {
        "name": "Mode",
        "display_name": "Mode",
        "gas_limits_file": "mode_gas_limits.csv",
        "default_gas_limit": CHAIN_DEFAULT_GAS_LIMIT,
        "color": "#DFFE00",  # Mode yellow-green
        "priority": 3,
        "enabled": True,
        "identifier": "mainnet/mode"
    },
    "zora": {
        "name": "Zora",
        "display_name": "Zora",
        "gas_limits_file": "zora_gas_limits.csv",
        "default_gas_limit": CHAIN_DEFAULT_GAS_LIMIT,
        "color": "#000000",  # Zora black
        "priority": 4,
        "enabled": True,
        "identifier": "mainnet/zora"
    },
    "world": {
        "name": "World",
        "display_name": "World Chain",
        "gas_limits_file": "world_gas_limits.csv",
        "default_gas_limit": CHAIN_DEFAULT_GAS_LIMIT,
        "color": "#00D4FF",  # World cyan
        "priority": 5,
        "enabled": True,
        "identifier": "mainnet/worldchain"
    },
    "ink": {
        "name": "Ink",
        "display_name": "Ink",
        "gas_limits_file": "ink_gas_limits.csv",
        "default_gas_limit": CHAIN_DEFAULT_GAS_LIMIT,
        "color": "#2D2D2D",  # Ink dark gray
        "priority": 6,
        "enabled": True,
        "identifier": "mainnet/ink"
    },
    "soneium": {
        "name": "Soneium",
        "display_name": "Soneium",
        "gas_limits_file": "soneium_gas_limits.csv",
        "default_gas_limit": CHAIN_DEFAULT_GAS_LIMIT,
        "color": "#9B59B6",  # Soneium purple
        "priority": 7,
        "enabled": True,
        "identifier": "mainnet/soneium"
    }
}

# Sampling methods
SAMPLING_METHODS = {
    "top_percentile": {
        "name": "Top Percentile",
        "description": "Select top X% blocks by calldata size",
        "default_percentile": 99.0  # Top 1%
    },
    "random": {
        "name": "Random Sampling",
        "description": "Randomly sample X% of blocks",
        "default_sample_rate": 1.0  # 1% sample
    },
    "stratified": {
        "name": "Stratified Sampling",
        "description": "Sample across calldata size buckets",
        "buckets": [0, 10000, 50000, 100000, 200000, 500000, 1000000],
        "samples_per_bucket": 100
    }
}

def get_enabled_chains() -> List[str]:
    """Get list of enabled chains."""
    return [chain for chain, config in CHAIN_CONFIGS.items() if config["enabled"]]

def get_chain_display_name(chain: str) -> str:
    """Get display name for a chain."""
    return CHAIN_CONFIGS.get(chain, {}).get("display_name", chain.capitalize())

def get_chain_color(chain: str) -> str:
    """Get color for a chain's visualizations."""
    return CHAIN_CONFIGS.get(chain, {}).get("color", "#808080")

def get_chain_identifier(chain: str) -> str:
    """Get ClickHouse identifier for a chain."""
    return CHAIN_CONFIGS.get(chain, {}).get("identifier", f"mainnet/{chain}")

def get_gas_limits_path(chain: str) -> Path:
    """Get path to gas limits CSV for a chain."""
    config = CHAIN_CONFIGS.get(chain)
    if not config:
        raise ValueError(f"Unknown chain: {chain}")

    base_path = Path(__file__).parent.parent / "gas_limits"
    return base_path / config["gas_limits_file"]


def get_default_gas_limit(chain: str) -> int:
    """Get default gas limit for a chain."""
    return CHAIN_CONFIGS.get(chain, {}).get("default_gas_limit", CHAIN_DEFAULT_GAS_LIMIT)
