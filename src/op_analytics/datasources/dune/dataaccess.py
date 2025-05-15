from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydata import DailyDataset

log = structlog.get_logger()


class Dune(DailyDataset):
    """Dune Datasets.

    This class includes utilities to read data from Dune queries
    """

    # DEX Trades
    DEX_TRADES = "daily_dex_trades_summary_v1"

    # Bespoke
    ETH_PRICE_VOL = "daily_eth_price_volatility_v1"

