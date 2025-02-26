from op_analytics.coreutils.partitioned.dailydata import DailyDataset


class Sugar(DailyDataset):
    TOKENS = "tokens_v1"
    POOLS = "liquidity_pools_v1"
    PRICES = "prices_v1"
