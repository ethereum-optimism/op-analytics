"""
Centralized schemas and harmonization utilities for the chain metadata pipeline.
"""

import polars as pl
from polars._typing import PolarsDataType

# Single source of truth for the canonical output schema.
CHAIN_METADATA_SCHEMA: dict[str, PolarsDataType] = {
    "chain": pl.Utf8,
    "chain_key": pl.Utf8,
    "chain_id": pl.Utf8,
    "layer": pl.Utf8,
    "display_name": pl.Utf8,
    "provider": pl.Utf8,
    "provider_entity": pl.Utf8,
    "is_evm": pl.Boolean,
    "op_governed_start": pl.Utf8,
    "is_current_chain": pl.Boolean,
    "is_upcoming": pl.Boolean,
    "l2b_da_layer": pl.Utf8,
    "l2b_stage": pl.Utf8,
    "min_dt_day": pl.Date,
    "max_dt_day": pl.Date,
    "data_sources": pl.List(pl.Utf8),
    "all_chain_keys": pl.List(pl.Utf8),
    "gas_token": pl.Utf8,
    "da_layer": pl.Utf8,
    "output_root_layer": pl.Utf8,
    "alignment": pl.Utf8,
    "provider_entity_w_superchain": pl.Utf8,
    "eth_eco_l2l3": pl.Boolean,
    "eth_eco_l2": pl.Boolean,
    # Keep legacy fields for now to aid in merging, will be dropped later.
    "source_name": pl.Utf8,
    "source_rank": pl.Int32,
}


DEFAULT_VALUES = {
    "is_current_chain": True,
    "is_upcoming": False,
    "eth_eco_l2l3": False,
    "eth_eco_l2": False,
    "is_evm": True,  # Most chains are EVM
    "layer": "L1",  # Default to L1 if not specified
    "provider": None,
    "provider_entity": None,
    "provider_entity_w_superchain": "Other",  # Keep as "Other" to match BQ pattern
    "alignment": None,
    "gas_token": "ETH",
    "da_layer": "Ethereum",
    "output_root_layer": "Ethereum",
    "l2b_da_layer": "Ethereum",
    "l2b_stage": "Not applicable",
}


def generate_chain_key(source_field: str) -> pl.Expr:
    """
    Generate a consistent chain_key from a source field.

    Args:
        source_field: The name of the column to use for generating the chain_key

    Returns:
        A Polars expression that generates a normalized chain_key
    """
    return (
        pl.col(source_field).str.to_lowercase().str.replace_all(r"[^a-z0-9]", "").alias("chain_key")
    )
