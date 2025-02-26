from op_analytics.coreutils.partitioned.dailydata import DailyDataset


class Velodrome(DailyDataset):
    """
    The Sugar dataset tracks tokens, pools, and prices from the Velodrome sugar-sdk.
    See also:
    - https://github.com/velodrome-finance/sugar
    - https://github.com/velodrome-finance/sugar-sdk

    Tables:
      - tokens_v1
      - pools_v1
      - prices_v1
    """

    TOKENS = "tokens_v1"
    POOLS = "pools_v1"
    PRICES = "prices_v1"
