"""
Centralized schemas and harmonization utilities for the chain metadata pipeline.
"""

import polars as pl
from op_analytics.coreutils.logger import structlog
from polars.type_aliases import PolarsDataType

log = structlog.get_logger()

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


def harmonize_to_canonical_schema(df: pl.DataFrame) -> pl.DataFrame:
    """
    Harmonizes a DataFrame to the canonical chain metadata schema.

    - Ensures all columns from CHAIN_METADATA_SCHEMA exist, adding them as null if missing.
    - Casts all existing columns to the correct canonical dtype, handling casting errors gracefully.
    - Selects and orders columns to exactly match the canonical schema definition.

    Args:
        df: The input Polars DataFrame to harmonize.

    Returns:
        A new DataFrame that conforms to the canonical schema.
    """
    if df.height == 0:
        log.warning(
            "Input DataFrame is empty. Returning an empty DataFrame with the canonical schema."
        )
        return pl.DataFrame(schema=CHAIN_METADATA_SCHEMA)

    output_df = df

    # Add missing columns as nulls
    for col_name, col_type in CHAIN_METADATA_SCHEMA.items():
        if col_name not in output_df.columns:
            output_df = output_df.with_columns(pl.lit(None, dtype=col_type).alias(col_name))

    # Cast existing columns to the correct type
    cast_expressions = []
    for col_name, col_type in CHAIN_METADATA_SCHEMA.items():
        if col_name in output_df.columns:
            cast_expressions.append(pl.col(col_name).cast(col_type, strict=False))

    output_df = output_df.with_columns(cast_expressions)

    # Select and order columns to match the schema
    final_cols = list(CHAIN_METADATA_SCHEMA.keys())
    output_df = output_df.select(final_cols)

    return output_df
