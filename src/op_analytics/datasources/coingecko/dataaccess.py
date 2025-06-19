"""
CoinGecko data source access definitions.
"""

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydata import DailyDataset

log = structlog.get_logger()


class CoinGecko(DailyDataset):
    """Supported CoinGecko datasets."""

    # Daily token price data
    DAILY_PRICES = "coingecko_daily_prices_v1"
