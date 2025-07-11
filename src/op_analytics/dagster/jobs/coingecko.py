"""
Dagster job for CoinGecko price data collection.
"""

from dagster import job, op, ScheduleDefinition

from op_analytics.coreutils.path import repo_path
from op_analytics.datasources.coingecko.execute import execute_pull


@op
def collect_coingecko_prices():
    """
    Collect daily price data from CoinGecko.
    """
    # Use the same parameters as your current setup
    # Fetch 365 days of data and include extra token IDs from config and top 100 tokens by market cap
    extra_token_ids_file = repo_path(
        "src/op_analytics/datasources/coingecko/config/extra_token_ids.txt"
    )
    return execute_pull(
        days=365,
        extra_token_ids_file=extra_token_ids_file,
        include_top_tokens=100,
        fetch_metadata=True,
        skip_existing_partitions=False,
        token_id=None,
    )


@job
def coingecko_price_job():
    """
    Job to collect daily price data from CoinGecko.
    """
    collect_coingecko_prices()


# Run daily at 00:30 UTC (12:30 AM)
coingecko_price_schedule = ScheduleDefinition(
    job=coingecko_price_job,
    cron_schedule="30 0 * * *",
    name="coingecko_price_schedule",
    description="Collect daily price data from CoinGecko",
)
