from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

import polars as pl
import yaml
import re

from op_analytics.coreutils.duckdb_inmem.client import init_client
from op_analytics.cli.subcommands.pulls.defillama.dataaccess import DefiLlama
from op_analytics.coreutils.logger import structlog

log = structlog.get_logger()


@dataclass
class DefiLlamaConfig:
    """
    Configuration container for DeFiLlama data processing.

    Attributes:
        exact_patterns_to_filter: List of exact chain names to filter
        ending_patterns_to_filter: List of chain name endings to filter
        categories_to_filter: List of protocol categories to exclude
        alignment_df: DataFrame mapping chains to their alignments
        token_categories: DataFrame of token categorization data
    """

    exact_patterns_to_filter: List[str]
    ending_patterns_to_filter: List[str]
    categories_to_filter: List[str]
    alignment_df: pl.DataFrame
    token_categories: pl.DataFrame


@dataclass
class DefillamaTVLBreakdown:
    """
    Container for processed DeFiLlama TVL breakdown data.

    Attributes:
        df_tvl_breakdown: DataFrame containing protocol token TVL data
    """

    df_tvl_breakdown: pl.DataFrame


def process_defillama_data() -> DefillamaTVLBreakdown:
    """
    Main function to process DeFiLlama protocol data.

    Returns:
        Processed and filtered DataFrame

    Raises:
        ValueError: If required data is missing or invalid
    """
    # Initialize
    ctx = init_client()
    client = ctx.client
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    three_days_ago = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
    config = load_config(
        "/Users/chuck/codebase/op-analytics/src/op_analytics/datapipeline/models/code/defillama_config.yaml"
    )

    # Load data
    view1 = DefiLlama.PROTOCOLS_TOKEN_TVL.read(min_date=three_days_ago)
    view2 = DefiLlama.PROTOCOLS_METADATA.read(min_date=yesterday)

    # Process protocol TVL
    df_protocol_tvl = pl.from_pandas(
        client.sql(f"""
        SELECT
            dt,
            protocol_slug,
            chain,
            token,
            app_token_tvl,
            app_token_tvl_usd
        FROM {view1}
    """).to_df()
    )

    # Process metadata
    df_metadata = pl.from_pandas(
        client.sql(f"""
        SELECT 
            protocol_name,
            protocol_slug,
            protocol_category,
            parent_protocol,
            CASE 
                WHEN misrepresented_tokens = 'True' THEN 1
                ELSE 0
            END AS misrepresented_tokens
        FROM {view2}
    """).to_df()
    )

    # Merge and process data
    df_all = df_protocol_tvl.unique().join(df_metadata.unique(), on="protocol_slug", how="left")

    # Join mappings and process data fields
    df_all = df_all.join(config.alignment_df, on="chain", how="left")
    df_all = df_all.join(config.token_categories, on="token", how="left")
    df_all = process_data_fields(df_all)

    # Process misrepresented tokens
    df_misrep = process_misrepresented_tokens(df_all)

    df_all = df_all.join(
        df_misrep.select(["protocol_slug", "chain", "protocol_misrepresented_tokens"]),
        on=["protocol_slug", "chain"],
        how="left",
    )

    df_all = df_all.with_columns(
        token_category_misrep=pl.when(pl.col("protocol_misrepresented_tokens") == 1)
        .then(pl.lit("Misrepresented TVL"))
        .otherwise(pl.col("token_category"))
    )

    # Apply protocol filters
    df_chain_protocol = apply_protocol_filters(df_all, config)

    df_tvl_breakdown = df_all.join(
        df_chain_protocol.select(["chain", "protocol_slug", "protocol_category"]),
        on=["chain", "protocol_slug", "protocol_category"],
        how="inner",
    )

    return DefillamaTVLBreakdown(df_tvl_breakdown=df_tvl_breakdown)


def load_config(config_path: Path) -> DefiLlamaConfig:
    """
    Loads and validates configuration from YAML file.

    Args:
        config_path: Path to configuration file

    Returns:
        DefiLlamaConfig object containing processed configuration

    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If YAML parsing fails
    """
    try:
        with open(config_path) as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Config file not found at {config_path}")
    except yaml.YAMLError as e:
        raise ValueError(f"Error parsing YAML configuration: {e}")

    # Create alignment DataFrame from the alignment dictionary
    alignment_df = pl.DataFrame(
        [
            {"chain": chain, "alignment": alignment}
            for chain, alignment in config["alignment_dict"].items()
        ]
    )

    # Create token categories DataFrame from token data
    token_categories = pl.DataFrame(config["token_data"])

    return DefiLlamaConfig(
        exact_patterns_to_filter=config["exact_patterns_to_filter"],
        ending_patterns_to_filter=config["ending_patterns_to_filter"],
        categories_to_filter=config["categories_to_filter"],
        alignment_df=alignment_df,
        token_categories=token_categories,
    )


def process_data_fields(df: pl.DataFrame) -> pl.DataFrame:
    """
    Processes and standardizes DataFrame fields.

    Args:
        df: Input DataFrame

    Returns:
        Processed DataFrame with standardized fields
    """
    return df.with_columns(
        [
            pl.col("alignment").fill_null("Other"),
            pl.col("token_category").fill_null("Other"),
            pl.col("chain").cast(pl.Utf8),
            pl.col("dt").cast(pl.Datetime),
            pl.col("parent_protocol").fill_null("").str.replace("parent#", ""),
            pl.col("token").fill_null("").str.to_uppercase(),
        ]
    )


def process_misrepresented_tokens(df: pl.DataFrame) -> pl.DataFrame:
    """
    Identifies and processes misrepresented tokens using DuckDB.

    Args:
        df: Polars DataFrame containing protocol and token data

    Returns:
        DataFrame with misrepresented token flags
    """
    # Initialize DuckDB connection
    ctx = init_client()
    client = ctx.client

    # Register the Polars DataFrame as a temporary view
    client.register("temp_df", df.to_pandas())

    result = pl.from_pandas(
        client.sql("""
        WITH latest_data AS (
            SELECT
                protocol_slug,
                chain,
                misrepresented_tokens,
                token,
                CASE WHEN UPPER(token) = 'USDT' THEN 1 ELSE 0 END AS is_usdt
            FROM temp_df
            WHERE dt = (SELECT MAX(dt) FROM temp_df) - INTERVAL '1' DAY
        )
        SELECT 
            protocol_slug,
            chain,
            misrepresented_tokens,
            CASE 
                WHEN misrepresented_tokens = 1 
                AND COUNT(DISTINCT token) = 1 
                AND MAX(is_usdt) = 1
                THEN 1 
                ELSE 0 
            END as protocol_misrepresented_tokens
        FROM latest_data
        GROUP BY protocol_slug, chain, misrepresented_tokens
    """).to_df()
    )

    # Clean up the temporary view
    client.execute("DROP VIEW IF EXISTS temp_df")

    return result


def apply_protocol_filters(df: pl.DataFrame, config: DefiLlamaConfig) -> pl.DataFrame:
    """
    Applies filtering logic to protocols using Polars expressions.

    Args:
        df: Polars DataFrame containing protocol data
        config: Configuration object containing filter patterns and categories

    Returns:
        DataFrame with protocol filter flags
    """
    # Create unique protocol entries
    filtered_df = df.unique(subset=["chain", "protocol_slug", "protocol_category"])

    # Check if chain ends with any of the ending patterns
    endings_pattern = "|".join(re.escape(e) for e in config.ending_patterns_to_filter)
    chain_ending_mask = pl.col("chain").str.to_lowercase().str.contains(rf"({endings_pattern})$")

    # Check if chain is exactly one of the exact patterns
    chain_exact_mask = (
        pl.col("chain")
        .str.to_lowercase()
        .is_in(pl.Series(config.exact_patterns_to_filter).str.to_lowercase())
    )

    # Check if protocol slug is exactly "polygon-bridge-&-staking"
    polygon_bridge_mask = pl.col("protocol_slug") == "polygon-bridge-&-staking"

    # Check if protocol slug ends with "-cex"
    cex_mask = pl.col("protocol_slug").str.ends_with("-cex")

    # Check if protocol category is in the list of categories to filter
    category_mask = pl.col("protocol_category").is_in(config.categories_to_filter)

    # Combine all masks
    all_filters = (
        chain_ending_mask | chain_exact_mask | polygon_bridge_mask | cex_mask | category_mask
    )

    return filtered_df.filter(~all_filters).select(["chain", "protocol_slug", "protocol_category"])
