"""Centralized schemas and harmonization utilities for chain metadata pipeline."""

import polars as pl
from polars._typing import PolarsDataType

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
    "source_name": pl.Utf8,  # Legacy field for merging
    "source_rank": pl.Int32,  # Legacy field for merging
}


DEFAULT_VALUES = {
    "is_current_chain": True,
    "is_upcoming": False,
    "eth_eco_l2l3": False,
    "eth_eco_l2": False,
    "is_evm": True,
    "layer": "L1",
    "provider": None,
    "provider_entity": None,
    "provider_entity_w_superchain": "Other",
    "alignment": None,
    "gas_token": "ETH",
    "da_layer": "Ethereum",
    "output_root_layer": "Ethereum",
    "l2b_da_layer": "Ethereum",
    "l2b_stage": "Not applicable",
}


def generate_chain_key(source_field: str) -> pl.Expr:
    """Generate a normalized chain key from source field."""
    return (
        pl.col(source_field)
        .str.to_lowercase()
        .str.replace_all(r"[^a-z0-9]", "_", literal=False)
        .str.replace_all(r"_+", "_", literal=False)
        .str.strip_chars("_")
        .alias("chain_key")
    )
