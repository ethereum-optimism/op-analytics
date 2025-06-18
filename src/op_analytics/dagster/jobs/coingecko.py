"""
Dagster job for CoinGecko price data collection.
"""

from dagster import job, op, ScheduleDefinition

from op_analytics.datasources.coingecko.execute import execute_pull


@op
def collect_coingecko_prices():
    """
    Collect daily price data from CoinGecko.
    """
    return execute_pull(days=30)


@job
def coingecko_price_job():
    """
    Job to collect daily price data from CoinGecko.
    """
    collect_coingecko_prices()


# Run daily at 00:00 UTC
coingecko_price_schedule = ScheduleDefinition(
    job=coingecko_price_job,
    cron_schedule="0 0 * * *",
    name="coingecko_price_schedule",
    description="Collect daily price data from CoinGecko",
)
