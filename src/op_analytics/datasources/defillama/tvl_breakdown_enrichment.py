import re
from dataclasses import dataclass
from datetime import date, timedelta

import polars as pl

from op_analytics.coreutils.duckdb_inmem.client import init_client
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydatautils import dt_summary
from op_analytics.coreutils.rangeutils.daterange import DateRange
from op_analytics.coreutils.time import date_fromstr, date_tostr, now_dt
from op_analytics.datasources.defillama.dataaccess import DefiLlama

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

DEFAULT_LOOKBACK_DAYS = 90


@dataclass
class DefillamaTVLBreakdown:
    df_tvl_breakdown: pl.DataFrame

    @classmethod
    def of_date(cls, current_dt: str | None = None, lookback_days: int = DEFAULT_LOOKBACK_DAYS):
        """Process DeFiLlama protocol data."""
        ctx = init_client()
        client = ctx.client

        current_dt = current_dt or now_dt()
        current_date = date_fromstr(current_dt)

        # We fetch the protocols metadata for "current_dt" - 1 day.
        # This ensures that the protocols metadata we use will be for a completed date.
        metadata_view = DefiLlama.PROTOCOLS_METADATA.read(
            min_date=current_date - timedelta(days=1),  # inclusive
            max_date=current_dt,  # exclusive
        )

        # We will transform the last "lookback_days" of data. We exclude "current_dt"
        # as that partition will be an incomplete data fetch from DefiLlama.
        min_date = current_date - timedelta(days=lookback_days)
        max_date = current_date
        tvl_view = DefiLlama.PROTOCOLS_TOKEN_TVL.read(
            min_date=min_date,  # inclusive
            max_date=max_date,  # exclusive
        )

        # Process protocol TVL
        df_protocol_tvl = client.sql(f"""
            SELECT
                dt,
                protocol_slug,
                chain,
                token,
                app_token_tvl,
                app_token_tvl_usd
            FROM {tvl_view}
        """).pl()

        # Process metadata
        df_metadata = client.sql(f"""
            SELECT 
                protocol_name,
                protocol_slug,
                protocol_category,
                parent_protocol,
                CASE 
                    WHEN misrepresented_tokens = 'True' THEN 1
                    ELSE 0
                END AS misrepresented_tokens
            FROM {metadata_view}
        """).pl()

        # Merge and process data
        df_all = df_protocol_tvl.unique().join(df_metadata.unique(), on="protocol_slug", how="left")

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
            df_chain_protocol.select(
                ["chain", "protocol_slug", "protocol_category", "to_filter_out"]
            ),
            on=["chain", "protocol_slug", "protocol_category"],
            how="inner",
        )

        data_quality_check(
            df_tvl_breakdown=df_tvl_breakdown,
            min_date=min_date,
            max_date=max_date,
        )

        return cls(df_tvl_breakdown=df_tvl_breakdown)


def data_quality_check(df_tvl_breakdown: pl.DataFrame, min_date: date, max_date: date):
    """Check that all expected "dt" partitions are present in the output data."""

    assert "dt" in df_tvl_breakdown.columns
    observed_dts = [date_tostr(_) for _ in sorted(df_tvl_breakdown["dt"].unique().to_list())]
    expected_dts = [date_tostr(_) for _ in DateRange(min=min_date, max=max_date).dates()]
    if observed_dts != expected_dts:
        missing_dts = sorted(set(expected_dts) - set(observed_dts))
        extra_dts = sorted(set(observed_dts) - set(expected_dts))

        summary = f"""
        MISSING dts:
        {'\n'.join(missing_dts)}
        
        EXTRA dts:
        {'\n'.join(extra_dts)}
        """
        raise Exception(f"possibly missing data after transformation: {summary}")


def execute_pull():
    result = DefillamaTVLBreakdown.of_date()

    # Write to storage
    DefiLlama.PROTOCOL_TOKEN_TVL_BREAKDOWN.write(
        dataframe=result.df_tvl_breakdown,
        sort_by=["chain", "protocol_slug", "token"],
    )

    return {
        "df_tvl_breakdown": dt_summary(result.df_tvl_breakdown),
    }


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
