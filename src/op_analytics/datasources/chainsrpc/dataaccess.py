from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydata import DailyDataset

log = structlog.get_logger()


class ChainsRPC(DailyDataset):
    """Tables that are updated daily using RPC calls."""

    ERC20_TOKEN_METADATA = "erc20_token_metadata_v1"
