import re

import polars as pl

from op_analytics.coreutils.duckdb_inmem.client import init_client
from op_analytics.coreutils.logger import structlog


log = structlog.get_logger()


ENDING_PATTERNS_TO_FILTER = ["-borrowed", "-vesting", "-staking", "-pool2", "-treasury", "-cex"]
EXACT_PATTERNS_TO_FILTER = ["treasury", "borrowed", "staking", "pool2", "polygon-bridge-&-staking"]
CATEGORIES_TO_FILTER = ["CEX", "Chain"]
EXCLUDE_CATEGORIES = [
    "RWA",
    "Basis Trading",
    "CeDeFi",
    "Chain",
    "CEX",
    "Infrastructure",
    "Staking Pool",
    "Bridge",
    "Yield Aggregator",
    "Yield",
    "Liquidity manager",
    "Managed Token Pools",
    "Treasury Manager",
    "Anchor BTC",
    "Liquid Staking",
    "Liquid Restaking",
]


def enrich_all(df: pl.DataFrame, metadata_df: pl.DataFrame) -> pl.DataFrame:
    # Merge and process data
    try:
        df_all = df.join(metadata_df, on="protocol_slug", how="left", validate="m:1")

    except Exception as ex:
        duplicate_protocols = (
            metadata_df.group_by(["protocol_slug"])
            .len()
            .filter(pl.col("len") > 1)["protocol_slug"]
            .to_list()
        )
        raise Exception(f"duplicate protocols found on metadata_df: {duplicate_protocols}") from ex

    # Process data fields
    df_all = process_data_fields(df_all)

    # Process misrepresented tokens
    df_misrep = process_misrepresented_tokens(df_all)

    df_all = df_all.join(
        df_misrep.select(["dt", "protocol_slug", "chain", "is_protocol_misrepresented"]),
        on=["dt", "protocol_slug", "chain"],
        how="left",
    )

    # Calculate double-counted TVL
    df_all = calculate_double_counted_tvl(df_all)

    # Apply protocol filters
    df_chain_protocol = create_filter_column(df_all)

    df_tvl_breakdown = df_all.join(
        df_chain_protocol.select(["chain", "protocol_slug", "protocol_category", "to_filter_out"]),
        on=["chain", "protocol_slug", "protocol_category"],
        how="inner",
    )

    log.info(f"filtered out breakdown {len(df_all)} -> {len(df_tvl_breakdown)}")
    return df_tvl_breakdown


def process_data_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Process and standardize DataFrame fields."""

    return df.with_columns(
        [
            pl.col("chain").cast(pl.Utf8),
            pl.col("dt").cast(pl.Datetime),
            pl.col("parent_protocol").fill_null("").str.replace("parent#", ""),
            pl.col("token").fill_null("").str.to_uppercase(),
        ]
    )


def process_misrepresented_tokens(df: pl.DataFrame) -> pl.DataFrame:
    """Identify and process misrepresented tokens using DuckDB.

    Args:
        df: Polars DataFrame containing protocol and token data

    Returns:
        DataFrame with misrepresented token flags
    """
    # Initialize DuckDB connection
    ctx = init_client()
    client = ctx.client

    # Register the Polars DataFrame as a temporary view
    client.register("temp_df", df)

    result = client.sql("""
        WITH latest_data AS (
            SELECT
                dt,
                protocol_slug,
                chain,
                misrepresented_tokens,
                token,
                CASE WHEN UPPER(token) = 'USDT' THEN 1 ELSE 0 END AS is_usdt
            FROM temp_df
        )
        SELECT 
            dt,
            protocol_slug,
            chain,
            misrepresented_tokens,
            CASE 
                WHEN misrepresented_tokens = 1 
                AND COUNT(DISTINCT token) = 1 
                AND MAX(is_usdt) = 1
                THEN 1 
                ELSE 0 
            END as is_protocol_misrepresented
        FROM latest_data
        GROUP BY dt, protocol_slug, chain, misrepresented_tokens
    """).pl()

    # Clean up the temporary view
    client.execute("DROP VIEW IF EXISTS temp_df")

    return result


def calculate_double_counted_tvl(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        is_double_counted=pl.col("protocol_category").is_in(EXCLUDE_CATEGORIES).cast(pl.Int8)
    )


def create_filter_column(df: pl.DataFrame) -> pl.DataFrame:
    """Add filtering flags to protocols using Polars expressions.

    Args:
        df: Polars DataFrame containing protocol data
        config: Configuration object containing filter patterns and categories

    Returns:
        DataFrame with "to_filter" column indicating which protocols should be filtered
    """
    # Create unique protocol entries
    filtered_df = df.unique(subset=["chain", "protocol_slug", "protocol_category"])

    # Check if chain ends with any of the ending patterns
    endings_pattern = "|".join(re.escape(e) for e in ENDING_PATTERNS_TO_FILTER)
    chain_ending_mask = pl.col("chain").str.to_lowercase().str.contains(rf"({endings_pattern})$")

    # Check if chain is exactly one of the exact patterns
    chain_exact_mask = (
        pl.col("chain")
        .str.to_lowercase()
        .is_in(pl.Series(EXACT_PATTERNS_TO_FILTER).str.to_lowercase())
    )

    # Check if protocol slug is exactly "polygon-bridge-&-staking"
    polygon_bridge_mask = pl.col("protocol_slug") == "polygon-bridge-&-staking"

    # Check if protocol slug ends with "-cex"
    cex_mask = pl.col("protocol_slug").str.ends_with("-cex")

    # Check if protocol category is in the list of categories to filter
    category_mask = pl.col("protocol_category").is_in(CATEGORIES_TO_FILTER)

    # Combine all masks
    all_masks = (
        chain_ending_mask | chain_exact_mask | polygon_bridge_mask | cex_mask | category_mask
    )

    # Combine all masks into a to_filter column and cast to integer
    return filtered_df.with_columns(to_filter_out=all_masks.cast(pl.Int8)).select(
        ["chain", "protocol_slug", "protocol_category", "to_filter_out"]
    )
