from op_analytics.coreutils.partitioned.dailydata import DailyDataset


class SugarDataAccess(DailyDataset):
    TOKENS = "sugar_tokens_v1"
    POOLS = "sugar_liquidity_pools_v1"
    PRICES = "sugar_prices_v1"
