"""
Execute CoinGecko price data collection.
"""

from typing import List

import polars as pl

from op_analytics.coreutils.bigquery.write import most_recent_dates
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.misc import raise_for_schema_mismatch
from op_analytics.coreutils.partitioned.dailydatautils import dt_summary
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

    # Get unique non-null CoinGecko API keys
    token_ids = (
        chain_metadata.filter(pl.col("cgt_coingecko_api_key").is_not_null())
        .select("cgt_coingecko_api_key")
        .unique()
        .to_series()
        .to_list()
    )

    log.info("found_token_ids", count=len(token_ids))
    return token_ids


def execute_pull(days: int = 30):
    """
    Execute the CoinGecko price data pull.

    Args:
        days: Number of days of historical data to fetch

    Returns:
        Dictionary with summary of pulled data
    """
    session = new_session()
    data_source = CoinGeckoDataSource(session=session)

    # Get list of token IDs
    token_ids = get_token_ids_from_metadata()
    if not token_ids:
        log.error("no_token_ids_found")
        return {}

    # Fetch price data
    try:
        price_df = data_source.get_token_prices(token_ids, days=days)
    except Exception as e:
        log.error("failed_to_fetch_price_data", error=str(e))
        raise

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

    return {
        "price_df": dt_summary(price_df),
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fetch CoinGecko price data")
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days of historical data to fetch",
    )
    args = parser.parse_args()

    execute_pull(days=args.days)
