"""
Execute CoinGecko price data collection.
"""

from typing import List, Dict, Any
import os
import csv

import polars as pl

from op_analytics.coreutils.bigquery.write import most_recent_dates
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.misc import raise_for_schema_mismatch
from op_analytics.coreutils.partitioned.dailydatautils import dt_summary
from op_analytics.coreutils.request import new_session
from op_analytics.coreutils.time import now_dt
from op_analytics.datapipeline.chains.load import load_chain_metadata

from .dataaccess import CoinGecko
from .price_data import CoinGeckoDataSource

log = structlog.get_logger()


PRICE_TABLE_LAST_N_DAYS = 90


PRICE_DF_SCHEMA = {
    "token_id": pl.String,
    "dt": pl.String,
    "price_usd": pl.Float64,
    "market_cap_usd": pl.Float64,
    "total_volume_usd": pl.Float64,
    "last_updated": pl.String,
}

METADATA_DF_SCHEMA = {
    "dt": pl.String,
    "token_id": pl.String,
    "name": pl.String,
    "symbol": pl.String,
    "description": pl.String,
    "categories": pl.List(pl.String),
    "homepage": pl.List(pl.String),
    "blockchain_site": pl.List(pl.String),
    "official_forum_url": pl.List(pl.String),
    "chat_url": pl.List(pl.String),
    "announcement_url": pl.List(pl.String),
    "twitter_screen_name": pl.String,
    "telegram_channel_identifier": pl.String,
    "subreddit_url": pl.String,
    "repos_url": pl.String,  # JSON string of platform -> repo URLs mapping
    "contract_addresses": pl.String,  # JSON string of platform -> address mapping
    "last_updated": pl.String,
}


def get_token_ids_from_metadata() -> List[str]:
    """
    Get list of CoinGecko token IDs from the chain metadata.

    Returns:
        List of CoinGecko token IDs
    """
    # Load chain metadata
    chain_metadata = load_chain_metadata()

    # Get token IDs from chain metadata
    token_ids = (
        chain_metadata.filter(pl.col("cgt_coingecko_api").is_not_null())
        .select("cgt_coingecko_api")
        .unique()
        .to_series()
        .to_list()
    )

    log.info("found_token_ids", count=len(token_ids))
    return token_ids


def read_token_ids_from_file(filepath: str) -> list[str]:
    """
    Read token IDs from a CSV or TXT file. Assumes one token_id per line or a column named 'token_id'.
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")
    token_ids: set[str] = set()
    ext = os.path.splitext(filepath)[1].lower()
    if ext == ".txt":
        with open(filepath, "r") as f:
            for line in f:
                tid = line.strip()
                if tid:
                    token_ids.add(tid)
    elif ext == ".csv":
        with open(filepath, newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            if reader.fieldnames and "token_id" in reader.fieldnames:
                for row in reader:
                    tid = row["token_id"].strip()
                    if tid:
                        token_ids.add(tid)
            else:
                # fallback: treat as single-column CSV
                csvfile.seek(0)
                reader = csv.reader(csvfile)
                for row in reader:
                    if row:  # Check if row is not empty
                        tid = row[0].strip()
                        if tid and tid != "token_id":
                            token_ids.add(tid)
    else:
        raise ValueError(f"Unsupported file extension: {ext}")
    return list(token_ids)


def get_token_ids_from_metadata_and_file(
    extra_token_ids_file: str | None = None, include_top_tokens: int = 0
) -> list[str]:
    """
    Get unique list of CoinGecko token IDs from chain metadata and an optional file.

    Args:
        extra_token_ids_file: Optional path to file with extra token IDs
        include_top_tokens: Number of top tokens by market cap to include (0 for none)
    """
    token_ids: set[str] = set(get_token_ids_from_metadata())

    if extra_token_ids_file:
        extra_ids = read_token_ids_from_file(extra_token_ids_file)
        token_ids.update(extra_ids)

    if include_top_tokens > 0:
        # Fetch top tokens by market cap
        session = new_session()
        data_source = CoinGeckoDataSource(session=session)
        try:
            top_token_ids = data_source.get_top_tokens_by_market_cap(limit=include_top_tokens)
            token_ids.update(top_token_ids)
            log.info("Added top tokens by market cap", count=len(top_token_ids))
        except Exception as e:
            log.error("Failed to fetch top tokens by market cap", error=str(e))
            # Continue without top tokens if there's an error

    result = list(token_ids)
    log.info("final_token_ids", count=len(result))
    return result


def _fetch_and_write_metadata(
    token_ids: list[str], data_source: CoinGeckoDataSource
) -> Dict[str, Any] | None:
    """
    Fetch and write metadata for the given token IDs.

    Args:
        token_ids: List of token IDs to fetch metadata for
        data_source: CoinGecko data source instance

    Returns:
        Summary of the metadata operation or None if failed
    """
    try:
        print(f"Fetching metadata for {len(token_ids)} tokens...")
        metadata_df = data_source.get_token_metadata(token_ids)
        print(f"Successfully fetched metadata for {len(metadata_df)} tokens")
    except Exception as e:
        log.error("failed_to_fetch_metadata", error=str(e))
        print(f"Error fetching metadata: {e}")
        return None

    if not metadata_df.is_empty():
        # Convert contract_addresses dict to JSON string for storage
        metadata_df = metadata_df.with_columns(pl.col("contract_addresses").cast(pl.Utf8))

        # Add dt partition column (following DefiLlama pattern)
        metadata_df = metadata_df.with_columns(dt=pl.lit(now_dt()))

        # Reorder columns to match schema (dt should be first)
        metadata_df = metadata_df.select(
            [
                "dt",
                "token_id",
                "name",
                "symbol",
                "description",
                "categories",
                "homepage",
                "blockchain_site",
                "official_forum_url",
                "chat_url",
                "announcement_url",
                "twitter_screen_name",
                "telegram_channel_identifier",
                "subreddit_url",
                "repos_url",
                "contract_addresses",
                "last_updated",
            ]
        )

        # Schema assertions to help our future selves reading this code.
        raise_for_schema_mismatch(
            actual_schema=metadata_df.schema,
            expected_schema=pl.Schema(METADATA_DF_SCHEMA),  # type: ignore[arg-type]
        )

        # Write metadata (partitioned by dt, replaces existing data for that date)
        CoinGecko.TOKEN_METADATA.write(
            dataframe=metadata_df,
            sort_by=["token_id"],
        )

        # Create BigQuery external table for metadata (using default partition)
        CoinGecko.TOKEN_METADATA.create_bigquery_external_table()
        CoinGecko.TOKEN_METADATA.create_bigquery_external_table_at_latest_dt()

        print(f"Successfully wrote metadata for {len(metadata_df)} tokens")
        return dt_summary(metadata_df)
    else:
        print("No metadata to write")
        return None


def execute_metadata_pull(
    extra_token_ids_file: str | None = None,
    include_top_tokens: int = 0,
) -> Dict[str, Any] | None:
    """
    Execute the CoinGecko metadata pull only.

    Args:
        extra_token_ids_file: Optional path to file with extra token IDs
        include_top_tokens: Number of top tokens by market cap to include (0 for none)

    Returns:
        Summary of the metadata fetch operation
    """
    session = new_session()
    data_source = CoinGeckoDataSource(session=session)

    # Get list of token IDs (from metadata, optional file, and optionally top tokens)
    token_ids = get_token_ids_from_metadata_and_file(extra_token_ids_file, include_top_tokens)
    print(f"Fetching metadata for {len(token_ids)} tokens: {token_ids}")

    if not token_ids:
        log.error("no_token_ids_found")
        return None

    # Fetch and write metadata
    metadata_summary = _fetch_and_write_metadata(token_ids, data_source)

    # Return summary information following DefiLlama pattern
    return {
        "metadata_df": metadata_summary,
    }


def execute_pull(
    days: int = 365,
    extra_token_ids_file: str | None = None,
    include_top_tokens: int = 0,
    fetch_metadata: bool = False,
) -> Dict[str, Any] | None:
    """
    Execute the CoinGecko price data pull.

    Args:
        days: Number of days of historical data to fetch
        extra_token_ids_file: Optional path to file with extra token IDs
        include_top_tokens: Number of top tokens by market cap to include (0 for none)
        fetch_metadata: Whether to fetch and write token metadata

    Returns:
        The actual price_df with the fetched price data
    """
    session = new_session()
    data_source = CoinGeckoDataSource(session=session)

    # Get list of token IDs (from metadata, optional file, and optionally top tokens)
    token_ids = get_token_ids_from_metadata_and_file(extra_token_ids_file, include_top_tokens)
    print(f"Fetching data for {len(token_ids)} tokens: {token_ids}")

    if not token_ids:
        log.error("no_token_ids_found")
        return None

    # Fetch price data for all tokens (get_token_prices now handles multiple tokens properly)
    try:
        print(f"Fetching price data for {len(token_ids)} tokens...")
        price_df = data_source.get_token_prices(token_ids, days=days)
        print(f"Successfully fetched data for {len(price_df)} token-days")
        print(f"Date range: {price_df['dt'].min()} to {price_df['dt'].max()}")
        print(f"Unique dates: {price_df['dt'].n_unique()}")
    except Exception as e:
        log.error("failed_to_fetch_price_data", error=str(e))
        print(f"Error fetching price data: {e}")
        return None

    # Schema assertions to help our future selves reading this code.
    raise_for_schema_mismatch(
        actual_schema=price_df.schema,
        expected_schema=pl.Schema(PRICE_DF_SCHEMA),  # type: ignore[arg-type]
    )

    # Write prices
    print("Writing price data to GCS...")
    print(f"Original data: {len(price_df)} rows")

    filtered_price_df = most_recent_dates(
        price_df, n_dates=PRICE_TABLE_LAST_N_DAYS, date_column="dt"
    )
    print(f"After filtering to last {PRICE_TABLE_LAST_N_DAYS} days: {len(filtered_price_df)} rows")
    print(
        f"Filtered date range: {filtered_price_df['dt'].min()} to {filtered_price_df['dt'].max()}"
    )

    CoinGecko.DAILY_PRICES.write(
        dataframe=filtered_price_df,
        sort_by=["token_id"],
    )
    print("Successfully wrote price data to GCS")

    # Create BigQuery external table
    print("Creating BigQuery external table for price data...")
    CoinGecko.DAILY_PRICES.create_bigquery_external_table()
    print("Successfully created BigQuery external table for price data")

    # Fetch and write metadata if requested
    metadata_summary = None
    if fetch_metadata:
        metadata_summary = _fetch_and_write_metadata(token_ids, data_source)

    # Return summary information following DefiLlama pattern
    return {
        "price_df": dt_summary(price_df),
        "metadata_df": metadata_summary,
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fetch CoinGecko price data")
    parser.add_argument(
        "--days",
        type=int,
        default=365,
        help="Number of days of historical data to fetch",
    )
    parser.add_argument(
        "--extra-token-ids-file",
        type=str,
        default=None,
        help="Optional path to a file (csv or txt) with extra token IDs to include",
    )
    parser.add_argument(
        "--include-top-tokens",
        type=int,
        default=0,
        help="Number of top tokens by market cap to include (0 for none)",
    )
    parser.add_argument(
        "--fetch-metadata",
        action="store_true",
        help="Whether to fetch and write token metadata",
    )
    parser.add_argument(
        "--metadata-only",
        action="store_true",
        help="Fetch and write only metadata (no price data)",
    )
    args = parser.parse_args()

    if args.metadata_only:
        result = execute_metadata_pull(
            extra_token_ids_file=args.extra_token_ids_file,
            include_top_tokens=args.include_top_tokens,
        )
    else:
        result = execute_pull(
            days=args.days,
            extra_token_ids_file=args.extra_token_ids_file,
            include_top_tokens=args.include_top_tokens,
            fetch_metadata=args.fetch_metadata,
        )

    if result is not None:
        if "price_df" in result and result["price_df"]:
            print("Successfully processed price data")
        if result["metadata_df"]:
            print("Successfully processed metadata data")
