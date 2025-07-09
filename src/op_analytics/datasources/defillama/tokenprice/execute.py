"""
Execute DeFiLlama token price data collection.
"""

from typing import Dict, Any

import polars as pl

from op_analytics.coreutils.bigquery.write import most_recent_dates
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.misc import raise_for_schema_mismatch
from op_analytics.coreutils.partitioned.dailydatautils import dt_summary
from op_analytics.coreutils.request import new_session
from op_analytics.datapipeline.chains.tokens import get_token_ids_from_metadata_and_file

from ..dataaccess import DefiLlama
from .price_data import DefiLlamaTokenPrices, TOKEN_PRICES_SCHEMA

log = structlog.get_logger()


def execute_pull_current(
    extra_token_ids_file: str | None = None,
    token_id: str | None = None,
) -> Dict[str, Any] | None:
    """
    Execute DeFiLlama current price data pull.

    Args:
        extra_token_ids_file: Optional path to file with extra token IDs
        token_id: Optional single token ID to process (if provided, ignores other token sources)

    Returns:
        Summary of the price fetch operation
    """
    session = new_session()

    # Get list of token IDs
    if token_id is not None:
        # Use single token if provided
        token_ids = [token_id]
        log.info("single_token_mode", token_id=token_id)
    else:
        # Use normal token collection logic
        token_ids = get_token_ids_from_metadata_and_file(
            extra_token_ids_file=extra_token_ids_file, include_top_tokens=0, top_tokens_fetcher=None
        )

    log.info("current_price_pull_start", token_count=len(token_ids))

    if not token_ids:
        log.error("no_token_ids_found")
        return None

    # Fetch current prices
    try:
        log.info("fetching_current_prices", token_count=len(token_ids))
        price_data = DefiLlamaTokenPrices.fetch_prices_current(token_ids, session=session)

        if price_data.is_empty():
            log.warning("no_price_data_returned")
            return None

        log.info("fetched_current_prices", row_count=len(price_data))

    except Exception as e:
        log.error("failed_to_fetch_current_prices", error=str(e))
        return None

    # Schema assertions
    raise_for_schema_mismatch(
        actual_schema=price_data.df.schema,
        expected_schema=TOKEN_PRICES_SCHEMA,
    )

    # Write prices (current prices are typically just one day)
    log.info("writing_current_price_data", rows=len(price_data.df))

    DefiLlama.TOKEN_PRICES.write(
        dataframe=price_data.df,
        sort_by=["token_id"],
    )
    log.info("wrote_current_price_data")

    # Create BigQuery external table
    DefiLlama.TOKEN_PRICES.create_bigquery_external_table()
    log.info("created_price_external_table")

    # Return summary information
    return {
        "price_df": dt_summary(price_data.df),
    }


def execute_pull_historical(
    days: int = 30,
    extra_token_ids_file: str | None = None,
    token_id: str | None = None,
    start_timestamp: int | None = None,
    end_timestamp: int | None = None,
    span: int = 0,
    period: str = "1d",
    search_width: str | None = None,
    skip_existing_partitions: bool = False,
) -> Dict[str, Any] | None:
    """
    Execute DeFiLlama historical price data pull.

    Args:
        days: Number of days of historical data to fetch (ignored if start_timestamp is provided)
        extra_token_ids_file: Optional path to file with extra token IDs
        token_id: Optional single token ID to process (if provided, ignores other token sources)
        start_timestamp: Unix timestamp of earliest data point (optional)
        end_timestamp: Unix timestamp of latest data point (optional)
        span: Number of data points returned, defaults to 0 (all available)
        period: Duration between data points (e.g., '1d', '1h', '1w')
        search_width: Time range on either side to find price data
        skip_existing_partitions: Whether to skip writing partitions that already exist

    Returns:
        Summary of the price fetch operation
    """
    session = new_session()

    # Get list of token IDs
    if token_id is not None:
        # Use single token if provided
        token_ids = [token_id]
        log.info("single_token_mode", token_id=token_id)
    else:
        # Use normal token collection logic
        token_ids = get_token_ids_from_metadata_and_file(
            extra_token_ids_file=extra_token_ids_file, include_top_tokens=0, top_tokens_fetcher=None
        )

    log.info("historical_price_pull_start", token_count=len(token_ids))

    if not token_ids:
        log.error("no_token_ids_found")
        return None

    # Fetch historical prices
    try:
        log.info("fetching_historical_prices", token_count=len(token_ids))

        if start_timestamp is not None or end_timestamp is not None:
            # Use explicit timestamps
            price_data = DefiLlamaTokenPrices.fetch_prices_historical(
                token_ids=token_ids,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
                span=span,
                period=period,
                search_width=search_width,
                session=session,
            )
        else:
            # Use days parameter
            price_data = DefiLlamaTokenPrices.fetch_prices_by_days(
                token_ids=token_ids,
                days=days,
                session=session,
            )

        if price_data.is_empty():
            log.warning("no_price_data_returned")
            return None

        log.info(
            "fetched_historical_prices",
            row_count=len(price_data.df),
            date_range=f"{str(price_data.df['dt'].min())} to {str(price_data.df['dt'].max())}",
            unique_dates=price_data.df["dt"].n_unique(),
        )

    except Exception as e:
        log.error("failed_to_fetch_historical_prices", error=str(e))
        return None

    # Filter out existing partitions if requested
    if skip_existing_partitions and not price_data.df.is_empty():
        from op_analytics.coreutils.partitioned.dataaccess import init_data_access
        from op_analytics.coreutils.partitioned.dailydatawrite import (
            determine_location,
            MARKERS_TABLE,
        )
        from op_analytics.coreutils.partitioned.output import ExpectedOutput

        # Get unique dates from the fetched data
        unique_dates = price_data.df["dt"].unique().to_list()

        # Check which dates already have complete markers
        client = init_data_access()
        location = determine_location()

        existing_dates = []
        for dt in unique_dates:
            # Construct the expected output marker path for this date
            expected_output = ExpectedOutput(
                root_path=DefiLlama.TOKEN_PRICES.root_path,
                file_name="out.parquet",
                marker_path=f"{dt}/{DefiLlama.TOKEN_PRICES.root_path}",
            )

            if client.marker_exists(
                data_location=location,
                marker_path=expected_output.marker_path,
                markers_table=MARKERS_TABLE,
            ):
                existing_dates.append(dt)

        if existing_dates:
            # Filter out existing dates from the price data
            original_count = len(price_data.df)
            price_data.df = price_data.df.filter(~pl.col("dt").is_in(existing_dates))

            log.info(
                "filtered_existing_partitions",
                original_rows=original_count,
                filtered_rows=len(price_data.df),
                skipped_dates=len(existing_dates),
                skipped_date_list=existing_dates,
            )

            if price_data.df.is_empty():
                log.info("all_partitions_already_exist")
                return {
                    "price_df": None,
                }

    # Schema assertions
    raise_for_schema_mismatch(
        actual_schema=price_data.df.schema,
        expected_schema=TOKEN_PRICES_SCHEMA,
    )

    # Write prices
    log.info("writing_historical_price_data", rows=len(price_data.df))

    # Use the requested days parameter for filtering
    filtered_price_df = most_recent_dates(price_data.df, n_dates=days, date_column="dt")
    log.info(
        "filtered_price_data",
        filtered_rows=len(filtered_price_df),
        date_range=f"{str(filtered_price_df['dt'].min())} to {str(filtered_price_df['dt'].max())}",
    )

    DefiLlama.TOKEN_PRICES.write(
        dataframe=filtered_price_df,
        sort_by=["token_id"],
    )
    log.info("wrote_historical_price_data")

    # Create BigQuery external table
    DefiLlama.TOKEN_PRICES.create_bigquery_external_table()
    log.info("created_price_external_table")

    # Return summary information
    return {
        "price_df": dt_summary(price_data.df),
    }


def execute_pull_first_prices(
    extra_token_ids_file: str | None = None,
    token_id: str | None = None,
) -> Dict[str, Any] | None:
    """
    Execute DeFiLlama first recorded price data pull.

    Args:
        extra_token_ids_file: Optional path to file with extra token IDs
        token_id: Optional single token ID to process (if provided, ignores other token sources)

    Returns:
        Summary of the price fetch operation
    """
    session = new_session()

    # Get list of token IDs
    if token_id is not None:
        # Use single token if provided
        token_ids = [token_id]
        log.info("single_token_mode", token_id=token_id)
    else:
        # Use normal token collection logic
        token_ids = get_token_ids_from_metadata_and_file(
            extra_token_ids_file=extra_token_ids_file, include_top_tokens=0, top_tokens_fetcher=None
        )

    log.info("first_price_pull_start", token_count=len(token_ids))

    if not token_ids:
        log.error("no_token_ids_found")
        return None

    # Fetch first prices
    try:
        log.info("fetching_first_prices", token_count=len(token_ids))
        price_data = DefiLlamaTokenPrices.fetch_first_prices(token_ids, session=session)

        if price_data.is_empty():
            log.warning("no_price_data_returned")
            return None

        log.info("fetched_first_prices", row_count=len(price_data))

    except Exception as e:
        log.error("failed_to_fetch_first_prices", error=str(e))
        return None

    # Schema assertions
    raise_for_schema_mismatch(
        actual_schema=price_data.df.schema,
        expected_schema=TOKEN_PRICES_SCHEMA,
    )

    # Write prices
    log.info("writing_first_price_data", rows=len(price_data.df))

    DefiLlama.TOKEN_PRICES.write(
        dataframe=price_data.df,
        sort_by=["token_id"],
    )
    log.info("wrote_first_price_data")

    # Create BigQuery external table
    DefiLlama.TOKEN_PRICES.create_bigquery_external_table()
    log.info("created_price_external_table")

    # Return summary information
    return {
        "price_df": dt_summary(price_data.df),
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fetch DeFiLlama token price data")
    parser.add_argument(
        "--mode",
        choices=["current", "historical", "first"],
        default="historical",
        help="Mode of price fetching: current, historical, or first",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days of historical data to fetch (for historical mode)",
    )
    parser.add_argument(
        "--extra-token-ids-file",
        type=str,
        default=None,
        help="Optional path to a file (csv or txt) with extra token IDs to include",
    )
    parser.add_argument(
        "--token-id",
        type=str,
        default=None,
        help="Optional single token ID to process (if provided, ignores other token sources)",
    )
    parser.add_argument(
        "--start-timestamp",
        type=int,
        default=None,
        help="Unix timestamp of earliest data point (for historical mode)",
    )
    parser.add_argument(
        "--end-timestamp",
        type=int,
        default=None,
        help="Unix timestamp of latest data point (for historical mode)",
    )
    parser.add_argument(
        "--span",
        type=int,
        default=0,
        help="Number of data points returned (for historical mode)",
    )
    parser.add_argument(
        "--period",
        type=str,
        default="1d",
        help="Duration between data points (for historical mode)",
    )
    parser.add_argument(
        "--search-width",
        type=str,
        default=None,
        help="Time range on either side to find price data (for historical mode)",
    )
    parser.add_argument(
        "--skip-existing-partitions",
        action="store_true",
        help="Skip writing partitions that already exist (for historical mode)",
    )
    args = parser.parse_args()

    if args.mode == "current":
        result = execute_pull_current(
            extra_token_ids_file=args.extra_token_ids_file,
            token_id=args.token_id,
        )
    elif args.mode == "historical":
        result = execute_pull_historical(
            days=args.days,
            extra_token_ids_file=args.extra_token_ids_file,
            token_id=args.token_id,
            start_timestamp=args.start_timestamp,
            end_timestamp=args.end_timestamp,
            span=args.span,
            period=args.period,
            search_width=args.search_width,
            skip_existing_partitions=args.skip_existing_partitions,
        )
    elif args.mode == "first":
        result = execute_pull_first_prices(
            extra_token_ids_file=args.extra_token_ids_file,
            token_id=args.token_id,
        )

    if result is not None:
        log.info("completed_price_processing")
    else:
        log.error("failed_price_processing")
