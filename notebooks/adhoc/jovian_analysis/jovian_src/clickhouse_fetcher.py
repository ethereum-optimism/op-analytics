"""
Simplified Jovian ClickHouse fetcher for top percentile and random sampling.
Now with multiprocessing support for parallel date fetching.
"""

import polars as pl
import pandas as pd
from typing import Tuple, Optional, List, Dict
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from datetime import timezone
from .chain_config import get_gas_limits_path
from .constants import DEFAULT_GAS_LIMIT

# Import from parent package
from op_analytics.coreutils.clickhouse.client import run_query
from op_analytics.coreutils.env.vault import env_get


def load_gas_limits(csv_path) -> pl.DataFrame:
    """Load gas limits from CSV file for a specific chain."""
    # Look for gas limits file in simplified structure

    # Read CSV with pandas for date parsing
    df_pandas = pd.read_csv(csv_path)

    # Convert to polars and rename columns
    df = pl.from_pandas(df_pandas)
    df = df.rename({
        "Date(UTC)": "date_str",
        "UnixTimeStamp": "unix_timestamp",
        "Value": "gas_limit"
    })

    # Parse dates and convert to YYYY-MM-DD
    df = df.with_columns([
        pl.col("date_str").str.strptime(pl.Date, "%m/%d/%Y").alias("date"),
        pl.col("gas_limit").cast(pl.Int64),
        pl.col("unix_timestamp").cast(pl.Int64)
    ])

    # Add formatted date string
    df = df.with_columns(
        pl.col("date").dt.strftime("%Y-%m-%d").alias("date_formatted")
    )

    return df.select(["date", "date_formatted", "unix_timestamp", "gas_limit"])


def get_gas_limit_for_date(date_str: str, gas_limits_df: pl.DataFrame, chain: str = "base") -> int:
    """Get gas limit for a specific date, default to 240M if not found."""
    result = gas_limits_df.filter(pl.col("date_formatted") == date_str)

    if len(result) > 0:
        gas_limit = result["gas_limit"][0]
        if gas_limit > 0:
            return int(gas_limit)

    # Default gas limit
    return None


def fetch_random_sample_blocks(
    chain: str,
    date: str,
    num_blocks: Optional[int] = None,
    sample_fraction: Optional[float] = None,  # e.g. 0.01 for 1%
    gas_limit: Optional[int] = None,
    seed: Optional[int] = None,
    start_datetime: Optional[datetime] = None,
    end_datetime: Optional[datetime] = None
) -> Tuple[pl.DataFrame, int]:
    """
    Fetch a random sample of blocks from a given day.

    If sample_fraction is provided (0< f ≤1), we select ceil(f * total_blocks) blocks
    using a deterministic hash-based ordering (seeded). Otherwise we select num_blocks.
    """

    # Get gas limit if not provided
    if gas_limit is None:
        gas_limits_df = load_gas_limits(get_gas_limits_path(chain))
        gas_limit = get_gas_limit_for_date(date, gas_limits_df, chain)

    # Get GCS credentials
    KEY_ID = env_get("GCS_HMAC_ACCESS_KEY")
    SECRET = env_get("GCS_HMAC_SECRET")

    # Deterministic hash-based ordering for sampling (faster than full rand())
    order_expr = (
        f"bitXor(cityHash64(block_number), toUInt64({seed}))" if seed is not None
        else "cityHash64(block_number)"
    )

    if start_datetime and end_datetime:
        # If naïve but represent UTC, attach UTC tzinfo without shifting
        if start_datetime.tzinfo is None:
            start_datetime = start_datetime.replace(tzinfo=timezone.utc)
        else:
            start_datetime = start_datetime.astimezone(timezone.utc)

        if end_datetime.tzinfo is None:
            end_datetime = end_datetime.replace(tzinfo=timezone.utc)
        else:
            end_datetime = end_datetime.astimezone(timezone.utc)

        start_ts = int(start_datetime.timestamp())
        end_ts   = int(end_datetime.timestamp())
        time_filter = f"block_timestamp >= {start_ts} AND block_timestamp <= {end_ts}"
    else:
        time_filter = "1"

    # Clamp/normalize fraction on the Python side for safety
    use_fraction = None
    if sample_fraction is not None:
        use_fraction = max(0.0, min(1.0, float(sample_fraction)))

    if use_fraction and use_fraction > 0.0:
        # One-shot query that computes K = ceil(f * total_blocks) and takes first K by deterministic order
        query = f"""
        WITH all_blocks AS (
            SELECT DISTINCT block_number, block_timestamp
            FROM s3(
                'https://storage.googleapis.com/oplabs-tools-data-sink/ingestion/transactions_v1/chain={chain}/dt={date}/*.parquet',
                '{KEY_ID}','{SECRET}','parquet'
            )
        ),
        sampled_ordered AS (
            SELECT block_number, block_timestamp,
                   row_number() OVER (ORDER BY {order_expr}) AS rn
            FROM all_blocks
            WHERE {time_filter}
        ),
        sample_target AS (
            SELECT greatest(toUInt64(1), toUInt64(ceil(count() * {use_fraction}))) AS k
            FROM all_blocks
            WHERE {time_filter}
        ),
        sampled_blocks AS (
            SELECT so.block_number, so.block_timestamp
            FROM sampled_ordered so
            CROSS JOIN sample_target st
            WHERE so.rn <= st.k
        ),
        block_stats AS (
            SELECT
                t.block_number,
                t.block_timestamp,
                SUM((LENGTH(t.input) / 2) - 1) AS block_total_calldata,
                SUM(t.receipt_gas_used)       AS block_total_gas_used
            FROM s3(
                'https://storage.googleapis.com/oplabs-tools-data-sink/ingestion/transactions_v1/chain={chain}/dt={date}/*.parquet',
                '{KEY_ID}','{SECRET}','parquet'
            ) t
            INNER JOIN sampled_blocks sb ON t.block_number = sb.block_number
            GROUP BY t.block_number, t.block_timestamp
        )
        ,blocks_with_base_fee AS (
            SELECT
                b.number,
                b.base_fee_per_gas
            FROM s3(
                'https://storage.googleapis.com/oplabs-tools-data-sink/ingestion/blocks_v1/chain={chain}/dt={date}/*.parquet',
                '{KEY_ID}','{SECRET}','parquet'
            ) b
        )
        SELECT
            t.block_number as block_number,
            t.transaction_index,
            t.input,
            (LENGTH(t.input) / 2) - 1 AS calldata_size,
            bs.block_total_calldata,
            bs.block_total_gas_used,
            bs.block_timestamp,
            bbf.base_fee_per_gas
        FROM s3(
            'https://storage.googleapis.com/oplabs-tools-data-sink/ingestion/transactions_v1/chain={chain}/dt={date}/*.parquet',
            '{KEY_ID}','{SECRET}','parquet'
        ) t
        INNER JOIN block_stats bs ON t.block_number = bs.block_number
        INNER JOIN blocks_with_base_fee bbf ON t.block_number = bbf.number
        ORDER BY bs.block_timestamp, t.block_number, t.transaction_index
        SETTINGS use_hive_partitioning = 1
        """
    else:
        n = int(num_blocks or 100)
        query = f"""
        WITH all_blocks AS (
            SELECT DISTINCT block_number
            FROM s3(
                'https://storage.googleapis.com/oplabs-tools-data-sink/ingestion/transactions_v1/chain={chain}/dt={date}/*.parquet',
                '{KEY_ID}','{SECRET}','parquet'
            )
        ),
        sampled_blocks AS (
            SELECT block_number
            FROM all_blocks
            ORDER BY {order_expr}
            LIMIT {n}
        ),
        block_stats AS (
            SELECT
                t.block_number,
                SUM((LENGTH(t.input) / 2) - 1) AS block_total_calldata,
                SUM(t.receipt_gas_used)       AS block_total_gas_used
            FROM s3(
                'https://storage.googleapis.com/oplabs-tools-data-sink/ingestion/transactions_v1/chain={chain}/dt={date}/*.parquet',
                '{KEY_ID}','{SECRET}','parquet'
            ) t
            INNER JOIN sampled_blocks sb ON t.block_number = sb.block_number
            GROUP BY t.block_number
        )
        SELECT
            t.block_number,
            t.transaction_index,
            t.input,
            (LENGTH(t.input) / 2) - 1 AS calldata_size,
            bs.block_total_calldata,
            bs.block_total_gas_used
        FROM s3(
            'https://storage.googleapis.com/oplabs-tools-data-sink/ingestion/transactions_v1/chain={chain}/dt={date}/*.parquet',
            '{KEY_ID}','{SECRET}','parquet'
        ) t
        INNER JOIN block_stats bs ON t.block_number = bs.block_number
        ORDER BY t.block_number, t.transaction_index
        SETTINGS use_hive_partitioning = 1
        """

    result_df = run_query(instance="OPLABS", query=query)

    if result_df.is_empty():
        return pl.DataFrame(), gas_limit

    # Add metadata columns
    result_df = result_df.with_columns([
        pl.lit(gas_limit).alias("gas_limit"),
        pl.lit(chain).alias("chain"),
        pl.lit("random").alias("sampling_method")
    ])

    return result_df, gas_limit



def fetch_top_percentile_blocks(
    chain: str,
    date: str,
    percentile: float = 99.0,
    limit: Optional[int] = None,
    gas_limit: Optional[int] = None
) -> Tuple[pl.DataFrame, int]:
    """
    Fetch the top X% blocks by calldata size.

    Args:
        chain: Chain name
        date: Date in YYYY-MM-DD format
        percentile: Percentile threshold (99 = top 1%)
        limit: Optional limit on number of blocks
        gas_limit: Optional gas limit (will be looked up if not provided)

    Returns:
        Tuple of (DataFrame with transaction data, gas_limit used)
    """
    # Get gas limit if not provided
    if gas_limit is None:
        gas_limits_df = load_gas_limits(get_gas_limits_path(chain))
        gas_limit = get_gas_limit_for_date(date, gas_limits_df, chain)

    # Get GCS credentials
    KEY_ID = env_get("GCS_HMAC_ACCESS_KEY")
    SECRET = env_get("GCS_HMAC_SECRET")

    # Get percentile threshold
    threshold_query = f"""
    WITH block_sizes AS (
        SELECT
            block_number,
            SUM((LENGTH(input) / 2) - 1) AS total_calldata,
            SUM(receipt_gas_used) AS total_gas_used
        FROM s3(
            'https://storage.googleapis.com/oplabs-tools-data-sink/ingestion/transactions_v1/chain={chain}/dt={date}/*.parquet',
            '{KEY_ID}',
            '{SECRET}',
            'parquet'
        )
        GROUP BY block_number
    )
    SELECT quantile({percentile/100})(total_calldata) AS threshold
    FROM block_sizes
    SETTINGS use_hive_partitioning = 1
    """

    stats_df = run_query(instance="OPLABS", query=threshold_query)

    if stats_df.is_empty():
        return pl.DataFrame(), gas_limit

    threshold = stats_df['threshold'][0]

    # Get transactions from top percentile blocks
    limit_clause = f"LIMIT {limit}" if limit else ""

    data_query = f"""
    WITH top_blocks AS (
        SELECT
            block_number,
            SUM((LENGTH(input) / 2) - 1) AS total_calldata,
            SUM(receipt_gas_used) AS total_gas_used
        FROM s3(
            'https://storage.googleapis.com/oplabs-tools-data-sink/ingestion/transactions_v1/chain={chain}/dt={date}/*.parquet',
            '{KEY_ID}',
            '{SECRET}',
            'parquet'
        )
        GROUP BY block_number
        HAVING total_calldata >= {threshold}
        ORDER BY total_calldata DESC
        {limit_clause}
    )
    SELECT
        t.block_number,
        t.transaction_index,
        t.input,
        (LENGTH(t.input) / 2) - 1 AS calldata_size,
        tb.total_calldata AS block_total_calldata,
        tb.total_gas_used AS block_total_gas_used
    FROM s3(
        'https://storage.googleapis.com/oplabs-tools-data-sink/ingestion/transactions_v1/chain={chain}/dt={date}/*.parquet',
        '{KEY_ID}',
        '{SECRET}',
        'parquet'
    ) t
    INNER JOIN top_blocks tb ON t.block_number = tb.block_number
    ORDER BY tb.total_calldata DESC, t.block_number, t.transaction_index
    SETTINGS use_hive_partitioning = 1
    """

    result_df = run_query(instance="OPLABS", query=data_query)

    if result_df.is_empty():
        return pl.DataFrame(), gas_limit

    # Add metadata columns
    result_df = result_df.with_columns([
        pl.lit(gas_limit).alias("gas_limit"),
        pl.lit(chain).alias("chain"),
        pl.lit("top_percentile").alias("sampling_method")
    ])

    return result_df, gas_limit


def fetch_multiple_dates_parallel(
    chain: str,
    dates: List[str],
    sampling_method: str = "random",
    num_blocks: int = 100,
    percentile: float = 99.0,
    max_workers: int = 4,
    gas_limits: Optional[Dict[str, int]] = None,
    seed: Optional[int] = None,
    verbose: bool = True
) -> Dict[str, Tuple[pl.DataFrame, int]]:
    """
    Fetch data for multiple dates in parallel using ThreadPoolExecutor.

    Args:
        chain: Chain name
        dates: List of dates in YYYY-MM-DD format
        sampling_method: "random" or "top_percentile"
        num_blocks: Number of blocks for random sampling
        percentile: Percentile for top percentile sampling
        max_workers: Maximum number of parallel workers
        gas_limits: Optional dict of date -> gas_limit
        seed: Random seed for reproducible sampling
        verbose: Print progress messages

    Returns:
        Dictionary mapping date -> (DataFrame, gas_limit)
    """
    results = {}

    # Load gas limits if not provided
    if gas_limits is None:
        gas_limits_df = load_gas_limits(get_gas_limits_path(chain))
        gas_limits = {}
        for date in dates:
            gas_limits[date] = get_gas_limit_for_date(date, gas_limits_df, chain)

    # Define the fetch function based on sampling method
    def fetch_single_date(date: str) -> Tuple[str, pl.DataFrame, int]:
        gas_limit = gas_limits.get(date)

        if sampling_method == "top_percentile":
            df, actual_gas_limit = fetch_top_percentile_blocks(
                chain=chain,
                date=date,
                percentile=percentile,
                gas_limit=gas_limit
            )
        else:
            df, actual_gas_limit = fetch_random_sample_blocks(
                chain=chain,
                date=date,
                num_blocks=num_blocks,
                gas_limit=gas_limit,
                seed=seed
            )

        return date, df, actual_gas_limit

    # Execute fetches in parallel
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        futures = {executor.submit(fetch_single_date, date): date for date in dates}

        # Process completed tasks
        for future in as_completed(futures):
            date = futures[future]
            try:
                date_result, df, gas_limit = future.result()
                results[date_result] = (df, gas_limit)

                if verbose and not df.is_empty():
                    blocks = df['block_number'].n_unique()
                    txs = len(df)
                    print(f"✅ {date}: {blocks} blocks, {txs:,} transactions")
                elif verbose:
                    print(f"⚠️ {date}: No data")

            except Exception as e:
                print(f"❌ Error fetching {date}: {e}")
                results[date] = (pl.DataFrame(), gas_limits.get(date, DEFAULT_GAS_LIMIT))

    elapsed = time.time() - start_time
    if verbose:
        print(f"\n⚡ Parallel fetching completed in {elapsed:.1f}s for {len(dates)} dates")

    return results


def fetch_date_range_single_query(
    chain: str,
    dates: List[str],
    sampling_method: str = "random",
    num_blocks_per_day: int = 100,
    percentile: float = 99.0,
    gas_limit: int = DEFAULT_GAS_LIMIT,
    seed: Optional[int] = None
) -> pl.DataFrame:
    """
    Fetch data for multiple dates in a single optimized query.
    This is more efficient than multiple queries but less flexible.

    Args:
        chain: Chain name
        dates: List of dates in YYYY-MM-DD format
        sampling_method: "random" or "top_percentile"
        num_blocks_per_day: Number of blocks per day for random sampling
        percentile: Percentile for top percentile sampling
        gas_limit: Gas limit to use for all dates
        seed: Random seed for reproducible sampling

    Returns:
        DataFrame with data for all dates (includes 'date' column)
    """
    # Get GCS credentials
    KEY_ID = env_get("GCS_HMAC_ACCESS_KEY")
    SECRET = env_get("GCS_HMAC_SECRET")

    # Create date filter for WHERE clause
    date_filter_clause = "', '".join(dates)

    if sampling_method == "random":
        # Random sampling across multiple dates
        order_expr = (
            f"bitXor(cityHash64(concat(toString(block_number), dt)), toUInt64({seed}))"
            if seed else "cityHash64(concat(toString(block_number), dt))"
        )

        query = f"""
        WITH all_blocks AS (
            SELECT DISTINCT
                block_number,
                dt
            FROM s3(
                'https://storage.googleapis.com/oplabs-tools-data-sink/ingestion/transactions_v1/chain={chain}/dt={{'{date_filter}'}}.*/∗.parquet',
                '{KEY_ID}',
                '{SECRET}',
                'parquet'
            )
        ),
        sampled_blocks AS (
            SELECT
                block_number,
                dt,
                row_number() OVER (PARTITION BY dt ORDER BY {order_expr}) as rn
            FROM all_blocks
            QUALIFY rn <= {num_blocks_per_day}
        ),
        block_stats AS (
            SELECT
                t.block_number,
                t.dt,
                SUM((LENGTH(t.input) / 2) - 1) AS block_total_calldata,
                SUM(t.receipt_gas_used) AS block_total_gas_used
            FROM s3(
                'https://storage.googleapis.com/oplabs-tools-data-sink/ingestion/transactions_v1/chain={chain}/dt={{'{date_filter}'}}.*/∗.parquet',
                '{KEY_ID}',
                '{SECRET}',
                'parquet'
            ) t
            INNER JOIN sampled_blocks sb ON t.block_number = sb.block_number AND t.dt = sb.dt
            GROUP BY t.block_number, t.dt
        )
        SELECT
            t.block_number,
            t.transaction_index,
            t.input,
            (LENGTH(t.input) / 2) - 1 AS calldata_size,
            bs.block_total_calldata,
            bs.block_total_gas_used,
            t.dt as date
        FROM s3(
            'https://storage.googleapis.com/oplabs-tools-data-sink/ingestion/transactions_v1/chain={chain}/dt=*/∗.parquet',
            '{KEY_ID}',
            '{SECRET}',
            'parquet'
        ) t
        WHERE t.dt IN ('{date_filter_clause}')
        INNER JOIN block_stats bs ON t.block_number = bs.block_number AND t.dt = bs.dt
        ORDER BY t.dt, t.block_number, t.transaction_index
        SETTINGS use_hive_partitioning = 1
        """
    else:
        # Top percentile sampling across multiple dates
        query = f"""
        WITH block_sizes AS (
            SELECT
                block_number,
                dt,
                SUM((LENGTH(input) / 2) - 1) AS total_calldata,
                SUM(receipt_gas_used) AS total_gas_used
            FROM s3(
                'https://storage.googleapis.com/oplabs-tools-data-sink/ingestion/transactions_v1/chain={chain}/dt={{'{date_filter}'}}.*/∗.parquet',
                '{KEY_ID}',
                '{SECRET}',
                'parquet'
            )
            GROUP BY block_number, dt
        ),
        thresholds AS (
            SELECT
                dt,
                quantile({percentile/100})(total_calldata) AS threshold
            FROM block_sizes
            GROUP BY dt
        ),
        top_blocks AS (
            SELECT
                bs.block_number,
                bs.dt,
                bs.total_calldata,
                bs.total_gas_used
            FROM block_sizes bs
            INNER JOIN thresholds t ON bs.dt = t.dt
            WHERE bs.total_calldata >= t.threshold
        )
        SELECT
            t.block_number,
            t.transaction_index,
            t.input,
            (LENGTH(t.input) / 2) - 1 AS calldata_size,
            tb.total_calldata AS block_total_calldata,
            tb.total_gas_used AS block_total_gas_used,
            t.dt as date
        FROM s3(
            'https://storage.googleapis.com/oplabs-tools-data-sink/ingestion/transactions_v1/chain={chain}/dt=*/∗.parquet',
            '{KEY_ID}',
            '{SECRET}',
            'parquet'
        ) t
        WHERE t.dt IN ('{date_filter_clause}')
        INNER JOIN top_blocks tb ON t.block_number = tb.block_number AND t.dt = tb.dt
        ORDER BY t.dt, tb.total_calldata DESC, t.block_number, t.transaction_index
        SETTINGS use_hive_partitioning = 1
        """

    result_df = run_query(instance="OPLABS", query=query)

    if not result_df.is_empty():
        # Add metadata columns
        result_df = result_df.with_columns([
            pl.lit(gas_limit).alias("gas_limit"),
            pl.lit(chain).alias("chain"),
            pl.lit(sampling_method).alias("sampling_method")
        ])

    return result_df
