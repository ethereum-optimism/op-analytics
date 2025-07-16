#!/usr/bin/env python3
"""
Generate Sankey Diagram Dataset for Superchain Fee Flow Breakdowns

This script reads data from daily_superchain_health_mv table, processes fee flows
according to the specified logic, and outputs to both BigQuery and ClickHouse.

Final schema:
- chain_set (from display_name in BQ)
- source
- destination
- value
- pct_of_total_fees_usd

Example usage:
    python generate_sankey_fees_dataset.py --days 90
"""

import os
import sys

import pandas as pd
import polars as pl
from google.cloud import bigquery

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), "../../../../"))
from op_analytics.coreutils.bigquery.write import overwrite_unpartitioned_table
from op_analytics.coreutils.clickhouse.oplabs import run_statememt_oplabs, insert_oplabs
from op_analytics.coreutils.logger import bound_contextvars, memory_usage, structlog

log = structlog.get_logger()

# Constants
BQ_SOURCE_TABLE = "oplabs-tools-data.materialized_tables.daily_superchain_health_mv"
BQ_OUTPUT_DATASET = "test_outputs"  # Use test dataset for safety
BQ_OUTPUT_TABLE = "superchain_fees_sankey_v1"
CH_OUTPUT_DATABASE = "analytics"
CH_OUTPUT_TABLE = "superchain_fees_sankey_v1"


def get_source_data(days: int) -> str:
    """
    Generate SQL query to read source data from daily_superchain_health_mv.

    Args:
        days: Number of days to look back from latest date for aggregation

    Returns:
        SQL query string
    """
    return f"""
    WITH date_range AS (
        SELECT 
            DATE_SUB(MAX(dt), INTERVAL {days} DAY) as start_date,
            MAX(dt) as end_date
        FROM `{BQ_SOURCE_TABLE}`
    )
    SELECT 
        display_name as chain_set,
        
        -- Aggregate daily data over the specified period
        SUM(COALESCE(total_chain_fees_usd, 0)) as total_chain_fees_usd,
        SUM(COALESCE(revshare_estimated_usd, 0)) as revshare_estimated_usd,
        SUM(COALESCE(chain_governor_profit_estimated_usd, 0)) as chain_governor_profit_estimated_usd,
        SUM(COALESCE(usd_gas_costs_per_day, 0)) as usd_gas_costs_per_day,
        
        -- MEV fee components  
        SUM(COALESCE(total_mev_revenue_usd, 0)) as total_mev_revenue_usd,
        SUM(COALESCE(total_mev_fees_usd, 0)) as total_mev_fees_usd,
        
        -- App fee components
        SUM(COALESCE(total_app_revenue_usd_excl_stable_mev, 0)) as total_app_revenue_usd_excl_stable_mev,
        SUM(COALESCE(total_app_fees_usd_excl_stable_mev, 0)) as total_app_fees_usd_excl_stable_mev,
        
        -- Stablecoin fee components
        SUM(COALESCE(total_stablecoin_revenue_usd, 0)) as total_stablecoin_revenue_usd,
        SUM(COALESCE(total_stablecoin_fees_usd, 0)) as total_stablecoin_fees_usd,
        
        -- Validation metadata
        COUNT(*) as days_of_data,
        MIN(mv.dt) as start_date,
        MAX(mv.dt) as end_date
        
    FROM `{BQ_SOURCE_TABLE}` mv
    CROSS JOIN date_range dr
    WHERE mv.dt >= dr.start_date
    AND mv.dt <= dr.end_date
    AND display_name IS NOT NULL
    GROUP BY display_name
    HAVING total_chain_fees_usd > 0  -- Only include chains with fees
    """


def process_fee_flows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process the aggregated data to create fee flow edges for Sankey diagram.

    Logic per requirements:
    1. total_fees_usd = total_chain_fees_usd + total_mev_fees_usd + total_stablecoin_fees_usd + total_app_fees_usd_excl_stable_mev
    2. total_chain_fees_usd = revshare_estimated_usd + chain_governor_profit_estimated_usd + usd_gas_costs_per_day + remaining_chain_fees_usd
    3. total_app_fees_usd_excl_stable_mev = total_app_revenue_usd_excl_stable_mev + remaining_total_app_fees_usd_excl_stable_mev
    4. total_stablecoin_fees_usd = total_stablecoin_revenue_usd + remaining_total_stablecoin_fees_usd
    5. total_mev_fees_usd = total_mev_revenue_usd + remaining_mev_fees_usd

    Args:
        df: DataFrame with aggregated fee data by chain

    Returns:
        DataFrame with Sankey edges (source, destination, value, pct_of_total_fees_usd)
    """

    sankey_rows = []

    for _, row in df.iterrows():
        chain_set = row["chain_set"]

        # Calculate derived totals using actual column names
        # For now, use the fees directly - can add "remaining" logic later
        total_mev_fees_usd = row["total_mev_fees_usd"]
        total_stablecoin_fees_usd = row["total_stablecoin_fees_usd"]
        total_app_fees_usd_excl_stable_mev = row["total_app_fees_usd_excl_stable_mev"]

        # Calculate remaining chain fees
        known_chain_fees = (
            row["revshare_estimated_usd"]
            + row["chain_governor_profit_estimated_usd"]
            + row["usd_gas_costs_per_day"]
        )
        remaining_chain_fees_usd = max(0, row["total_chain_fees_usd"] - known_chain_fees)

        # Calculate remaining for other categories
        remaining_mev_fees_usd = max(0, total_mev_fees_usd - row["total_mev_revenue_usd"])
        remaining_stablecoin_fees_usd = max(
            0, total_stablecoin_fees_usd - row["total_stablecoin_revenue_usd"]
        )
        remaining_app_fees_usd = max(
            0, total_app_fees_usd_excl_stable_mev - row["total_app_revenue_usd_excl_stable_mev"]
        )

        # Calculate total fees
        total_fees_usd = (
            row["total_chain_fees_usd"]
            + total_mev_fees_usd
            + total_stablecoin_fees_usd
            + total_app_fees_usd_excl_stable_mev
        )

        if total_fees_usd <= 0:
            continue  # Skip chains with no fees

        # Create edges for Sankey diagram
        # Level 1: Total fees breakdown (these count toward 100%)
        level1_edges = []
        if row["total_chain_fees_usd"] > 0:
            level1_edges.append(
                (chain_set, "total_fees_usd", "total_chain_fees_usd", row["total_chain_fees_usd"])
            )
        if total_mev_fees_usd > 0:
            level1_edges.append(
                (chain_set, "total_fees_usd", "total_mev_fees_usd", total_mev_fees_usd)
            )
        if total_stablecoin_fees_usd > 0:
            level1_edges.append(
                (
                    chain_set,
                    "total_fees_usd",
                    "total_stablecoin_fees_usd",
                    total_stablecoin_fees_usd,
                )
            )
        if total_app_fees_usd_excl_stable_mev > 0:
            level1_edges.append(
                (
                    chain_set,
                    "total_fees_usd",
                    "total_app_fees_usd_excl_stable_mev",
                    total_app_fees_usd_excl_stable_mev,
                )
            )

        # Level 2: Detailed breakdowns (these do NOT count toward 100%, they're sub-flows)
        level2_edges = []

        # Chain fee breakdown
        if row["revshare_estimated_usd"] > 0:
            level2_edges.append(
                (
                    chain_set,
                    "total_chain_fees_usd",
                    "revshare_estimated_usd",
                    row["revshare_estimated_usd"],
                )
            )
        if row["chain_governor_profit_estimated_usd"] > 0:
            level2_edges.append(
                (
                    chain_set,
                    "total_chain_fees_usd",
                    "chain_governor_profit_estimated_usd",
                    row["chain_governor_profit_estimated_usd"],
                )
            )
        if row["usd_gas_costs_per_day"] > 0:
            level2_edges.append(
                (
                    chain_set,
                    "total_chain_fees_usd",
                    "usd_gas_costs_per_day",
                    row["usd_gas_costs_per_day"],
                )
            )
        if remaining_chain_fees_usd > 0:
            level2_edges.append(
                (
                    chain_set,
                    "total_chain_fees_usd",
                    "remaining_chain_fees_usd",
                    remaining_chain_fees_usd,
                )
            )

        # MEV fee breakdown
        if row["total_mev_revenue_usd"] > 0:
            level2_edges.append(
                (
                    chain_set,
                    "total_mev_fees_usd",
                    "total_mev_revenue_usd",
                    row["total_mev_revenue_usd"],
                )
            )
        if remaining_mev_fees_usd > 0:
            level2_edges.append(
                (chain_set, "total_mev_fees_usd", "remaining_mev_fees_usd", remaining_mev_fees_usd)
            )

        # App fee breakdown
        if row["total_app_revenue_usd_excl_stable_mev"] > 0:
            level2_edges.append(
                (
                    chain_set,
                    "total_app_fees_usd_excl_stable_mev",
                    "total_app_revenue_usd_excl_stable_mev",
                    row["total_app_revenue_usd_excl_stable_mev"],
                )
            )
        if remaining_app_fees_usd > 0:
            level2_edges.append(
                (
                    chain_set,
                    "total_app_fees_usd_excl_stable_mev",
                    "remaining_app_fees_usd_excl_stable_mev",
                    remaining_app_fees_usd,
                )
            )

        # Stablecoin fee breakdown
        if row["total_stablecoin_revenue_usd"] > 0:
            level2_edges.append(
                (
                    chain_set,
                    "total_stablecoin_fees_usd",
                    "total_stablecoin_revenue_usd",
                    row["total_stablecoin_revenue_usd"],
                )
            )
        if remaining_stablecoin_fees_usd > 0:
            level2_edges.append(
                (
                    chain_set,
                    "total_stablecoin_fees_usd",
                    "remaining_stablecoin_fees_usd",
                    remaining_stablecoin_fees_usd,
                )
            )

        # Add Level 1 edges (count toward 100%)
        for chain, source, destination, value in level1_edges:
            pct_of_total = (value / total_fees_usd) * 100
            sankey_rows.append(
                {
                    "chain_set": chain,
                    "source": source,
                    "destination": destination,
                    "value": value,
                    "pct_of_total_fees_usd": pct_of_total,
                }
            )

        # Add Level 2 edges (percentage based on their parent category, not total)
        for chain, source, destination, value in level2_edges:
            # Find the parent category value to calculate percentage
            if source == "total_chain_fees_usd":
                parent_value = row["total_chain_fees_usd"]
            elif source == "total_mev_fees_usd":
                parent_value = total_mev_fees_usd
            elif source == "total_stablecoin_fees_usd":
                parent_value = total_stablecoin_fees_usd
            elif source == "total_app_fees_usd_excl_stable_mev":
                parent_value = total_app_fees_usd_excl_stable_mev
            else:
                parent_value = total_fees_usd  # fallback

            # Calculate percentage as a sub-component (this represents the breakdown)
            if parent_value > 0:
                pct_of_parent = (value / parent_value) * 100
                # But we want to show what percentage this is of the total fees too
                pct_of_total = (value / total_fees_usd) * 100
                sankey_rows.append(
                    {
                        "chain_set": chain,
                        "source": source,
                        "destination": destination,
                        "value": value,
                        "pct_of_total_fees_usd": pct_of_total,  # This shows the sub-component as % of total
                    }
                )

    return pd.DataFrame(sankey_rows)


def validate_output(df: pd.DataFrame) -> None:
    """Validate the output dataset structure and percentages."""
    log.info("Validating output dataset")

    # Basic structure validation
    required_columns = ["chain_set", "source", "destination", "value", "pct_of_total_fees_usd"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        log.error("Missing required columns", missing_columns=missing_columns)
        raise ValueError(f"Missing required columns: {missing_columns}")

    log.info("Schema validation passed", columns=list(df.columns))

    # Percentage validation - Level 1 edges (main categories) should sum to 100% per chain
    level1_df = df[df["source"] == "total_fees_usd"]
    if len(level1_df) > 0:
        percentage_summary = level1_df.groupby("chain_set")["pct_of_total_fees_usd"].sum().round(1)

        log.info("Level 1 percentage validation")
        chains_not_100 = percentage_summary[percentage_summary != 100.0]
        if len(chains_not_100) > 0:
            log.error(
                "Level 1 percentages don't sum to 100%", invalid_chains=chains_not_100.to_dict()
            )
        else:
            log.info("All Level 1 percentages sum to 100%", chains_count=len(percentage_summary))

    # Edge type breakdown
    edge_types = df["source"].value_counts().to_dict()
    log.info("Edge type breakdown", total_edges=len(df), edge_types=edge_types)

    # Show sample Level 2 edges
    level2_df = df[df["source"] != "total_fees_usd"]
    if len(level2_df) > 0:
        sample_edges = level2_df.head(5)[
            ["chain_set", "source", "destination", "value", "pct_of_total_fees_usd"]
        ]
        log.info("Sample Level 2 edges", sample_data=sample_edges.to_dict("records"))


def write_to_bigquery(df: pd.DataFrame) -> None:
    """Write output to BigQuery."""
    log.info(
        "Writing to BigQuery",
        dataset=BQ_OUTPUT_DATASET,
        table=f"{BQ_OUTPUT_TABLE}_latest",
        rows=len(df),
    )

    # Convert to Polars for BigQuery write utility
    pl_df = pl.from_pandas(df)

    try:
        overwrite_unpartitioned_table(pl_df, BQ_OUTPUT_DATASET, f"{BQ_OUTPUT_TABLE}_latest")
        log.info("Successfully wrote to BigQuery")
    except Exception as e:
        log.error("Error writing to BigQuery", error=str(e))


def write_to_clickhouse(df: pd.DataFrame) -> None:
    """Write output to ClickHouse."""
    log.info(
        "Writing to ClickHouse", database=CH_OUTPUT_DATABASE, table=CH_OUTPUT_TABLE, rows=len(df)
    )

    # Create table if not exists
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {CH_OUTPUT_DATABASE}.{CH_OUTPUT_TABLE} (
        chain_set String,
        source String,
        destination String,
        value Float64,
        pct_of_total_fees_usd Float64,
        created_at DateTime DEFAULT now()
    ) ENGINE = ReplacingMergeTree(created_at)
    ORDER BY (chain_set, source, destination)
    """

    try:
        # Create table
        run_statememt_oplabs(create_table_sql)

        # Convert DataFrame to Arrow for ClickHouse
        import pyarrow as pa

        arrow_table = pa.Table.from_pandas(df)

        # Insert data
        insert_oplabs(CH_OUTPUT_DATABASE, CH_OUTPUT_TABLE, arrow_table)
        log.info("Successfully wrote to ClickHouse")
    except Exception as e:
        log.error("Error writing to ClickHouse", error=str(e))


def execute_pull(days: int = 90, dry_run: bool = False):
    """Generate Sankey diagram dataset for Superchain fee flows."""

    with bound_contextvars(pipeline_step="fees_sankey"):
        log.info("Starting Sankey fees dataset generation", days=days, dry_run=dry_run)

        # Initialize BigQuery client
        client = bigquery.Client()

        # Get source data
        log.info("Querying source data", table=BQ_SOURCE_TABLE)
        query = get_source_data(days)

        try:
            query_job = client.query(query)
            # Convert to pandas without db-dtypes
            rows = query_job.result()
            data = []
            for row in rows:
                data.append(dict(row))
            source_df = pd.DataFrame(data)

            log.info(
                "Retrieved chains with fee data",
                chains_count=len(source_df),
                max_rss=memory_usage(),
            )
            if len(source_df) > 0:
                log.info(
                    "Date range aggregation",
                    start_date=source_df["start_date"].min(),
                    end_date=source_df["end_date"].max(),
                    avg_days_per_chain=source_df["days_of_data"].mean(),
                    requested_days=days,
                )
                log.info(
                    "Fee data summary",
                    min_fees_usd=source_df["total_chain_fees_usd"].min(),
                    max_fees_usd=source_df["total_chain_fees_usd"].max(),
                    total_aggregated_fees=source_df["total_chain_fees_usd"].sum(),
                )

        except Exception as e:
            log.error("Error querying source data", error=str(e))
            raise

        # Process fee flows
        log.info("Processing fee flows")
        sankey_df = process_fee_flows(source_df)
        log.info("Generated fee flow edges", edges_count=len(sankey_df))

        if len(sankey_df) == 0:
            log.error("No fee flow edges generated")
            raise ValueError("No fee flow edges generated")

        # Validate output
        validate_output(sankey_df)

        # Show sample output
        log.info("Sample output preview")
        log.info(sankey_df.head(10).to_string(index=False))

        if not dry_run:
            # Write to databases
            write_to_bigquery(sankey_df)
            write_to_clickhouse(sankey_df)
        else:
            log.info("Dry run complete - no data written")

        log.info("Sankey fees dataset generation complete", max_rss=memory_usage())

        # Return summary following datasources pattern
        return {
            "chains_processed": len(source_df),
            "edges_generated": len(sankey_df),
            "level1_edges": len(sankey_df[sankey_df["source"] == "total_fees_usd"]),
            "level2_edges": len(sankey_df[sankey_df["source"] != "total_fees_usd"]),
            "dry_run": dry_run,
            "days_requested": days,
            "dataframe": sankey_df,  # Include DataFrame for prototyping/analysis
        }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate Sankey diagram dataset for Superchain fee flows"
    )
    parser.add_argument("--days", type=int, default=90, help="Number of days to look back")
    parser.add_argument(
        "--dry-run", action="store_true", help="Don't write to databases, just validate"
    )

    args = parser.parse_args()
    result = execute_pull(args.days, args.dry_run)
    log.info("Execution summary", **result)
