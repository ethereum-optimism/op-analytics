from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydata import DailyDataset

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

    # DEX Volumes, Fees, and Revenue at chain and chain/name levels of granularity
    VOLUME_FEES_REVENUE = "volume_fees_revenue_v1"
    VOLUME_FEES_REVENUE_BREAKDOWN = "volume_fees_revenue_breakdown_v1"

    # Yield
    YIELD_POOLS_METADATA = "yield_pools_metadata_v1"
    YIELD_POOLS_HISTORICAL = "yield_pools_historical_v1"

    # Lend/Borrow
    LEND_BORROW_POOLS_METADATA = "lend_borrow_pools_metadata_v1"
    LEND_BORROW_POOLS_HISTORICAL = "lend_borrow_pools_historical_v1"

    # Protocols metadata obtained from "dexs/dailyVolume", and "fees/dailyFees"
    # and "fees/dailyRevenue" endpoints.
    VOLUME_PROTOCOLS_METADATA = "volume_protocols_metadata_v1"
    FEES_PROTOCOLS_METADATA = "fees_protocols_metadata_v1"
    REVENUE_PROTOCOLS_METADATA = "revenue_protocols_metadata_v1"

    # TVL breakdown
    PROTOCOL_TOKEN_TVL_BREAKDOWN = "protocol_token_tvl_breakdown_v1"

    # Token Mappings
    TOKEN_MAPPINGS = "dim_token_mappings_v1"

    # Net TVL Flows
    PROTOCOL_TOKEN_NET_TVL_FLOWS = "net_tvl_flows_v1"
