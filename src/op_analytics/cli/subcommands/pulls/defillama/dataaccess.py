import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydata import (
    write_daily_data,
    DailyDataset,
)

log = structlog.get_logger()


class DefiLlama(DailyDataset):
    """Supported defillama datasets.

    This class includes utilities to read data from each dataset from a notebook
    for ad-hoc use cases.
    """

    # Chain TVL
    CHAINS_METADATA = "chains_metadata_v1"
    HISTORICAL_CHAIN_TVL = "historical_chain_tvl_v1"

    # Protocol TVL
    PROTOCOLS_METADATA = "protocols_metadata_v1"
    PROTOCOLS_TVL = "protocols_tvl_v1"
    PROTOCOLS_TOKEN_TVL = "protocols_token_tvl_v1"

    # Stablecoins TVL
    STABLECOINS_METADATA = "stablecoins_metadata_v1"
    STABLECOINS_BALANCE = "stablecoins_balances_v1"

    # DEX Volumes, Fees, and Revenue at various levels of granularity
    DEX_TOTAL = "dexs_fees_crypto_v1"
    DEX_CHAIN = "dexs_fees_chain_v1"
    DEX_BREAKDOWN = "dexs_fees_chain_protocol_v1"

    # Summary for dexs/dailyVolume, fees/dailyFees, and fees/dailyRevenue
    DEXS_VOLUME_SUMMARY = "dexs_volume_summary_v1"
    FEES_FEES_SUMMARY = "fees_fees_summary_v1"
    FEES_REVENUE_SUMMARY = "fees_revenue_summary_v1"

    def write(
        self,
        dataframe: pl.DataFrame,
        sort_by: list[str] | None = None,
    ):
        return write_daily_data(
            root_path=self.root_path,
            dataframe=dataframe,
            sort_by=sort_by,
        )
