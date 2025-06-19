"""
Execute CoinGecko price data collection.
"""

from typing import List
import os
import csv

import polars as pl

from op_analytics.coreutils.bigquery.write import most_recent_dates
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.misc import raise_for_schema_mismatch
from op_analytics.coreutils.request import new_session
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
    token_ids = set()
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
            if "token_id" in reader.fieldnames:
                for row in reader:
                    tid = row["token_id"].strip()
                    if tid:
                        token_ids.add(tid)
            else:
                # fallback: treat as single-column CSV
                csvfile.seek(0)
                for row in csvfile:
                    tid = row.strip()
                    if tid and tid != "token_id":
                        token_ids.add(tid)
    else:
        raise ValueError(f"Unsupported file extension: {ext}")
    return list(token_ids)


def get_token_ids_from_metadata_and_file(extra_token_ids_file: str | None = None) -> list[str]:
    """
    Get unique list of CoinGecko token IDs from chain metadata and an optional file.
    """
    token_ids = set(get_token_ids_from_metadata())
    if extra_token_ids_file:
        extra_ids = read_token_ids_from_file(extra_token_ids_file)
        token_ids.update(extra_ids)
    token_ids = list(token_ids)
    log.info("final_token_ids", count=len(token_ids))
    return token_ids


def execute_pull(days: int = 30, extra_token_ids_file: str | None = None):
    """
    Execute the CoinGecko price data pull.

    Args:
        days: Number of days of historical data to fetch
        extra_token_ids_file: Optional path to file with extra token IDs

    Returns:
        The actual price_df with the fetched price data
    """
    session = new_session()
    data_source = CoinGeckoDataSource(session=session)

    # Get list of token IDs (from metadata and optional file)
    token_ids = get_token_ids_from_metadata_and_file(extra_token_ids_file)
    print(f"Fetching data for {len(token_ids)} tokens: {token_ids}")

    if not token_ids:
        log.error("no_token_ids_found")
        return None

    # Fetch price data for all tokens (get_token_prices now handles multiple tokens properly)
    try:
        print(f"Fetching price data for {len(token_ids)} tokens...")
        price_df = data_source.get_token_prices(token_ids, days=days)
        print(f"Successfully fetched data for {len(price_df)} token-days")
    except Exception as e:
        log.error("failed_to_fetch_price_data", error=str(e))
        print(f"Error fetching price data: {e}")
        return None

    # Schema assertions to help our future selves reading this code.
    raise_for_schema_mismatch(
        actual_schema=price_df.schema,
        expected_schema=pl.Schema(PRICE_DF_SCHEMA),
    )

    # Write prices
    CoinGecko.DAILY_PRICES.write(
        dataframe=most_recent_dates(price_df, n_dates=PRICE_TABLE_LAST_N_DAYS, date_column="dt"),
        sort_by=["token_id"],
    )

    # Create BigQuery external table
    CoinGecko.DAILY_PRICES.create_bigquery_external_table()

    return price_df


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fetch CoinGecko price data")
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days of historical data to fetch",
    )
    parser.add_argument(
        "--extra-token-ids-file",
        type=str,
        default=None,
        help="Optional path to a file (csv or txt) with extra token IDs to include",
    )
    args = parser.parse_args()

    execute_pull(days=args.days, extra_token_ids_file=args.extra_token_ids_file)
