from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydata import DailyDataset

log = structlog.get_logger()


class EthereumOptimism(DailyDataset):
    """Read data from ethereum-optimism github repo."""

    # Superchain Token List
    SUPERCHAIN_TOKEN_LIST = "superchain_token_list_v1"
