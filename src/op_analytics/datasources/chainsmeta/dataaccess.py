from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydata import DailyDataset

log = structlog.get_logger()


class ChainsMeta(DailyDataset):
    """Metadata about onchain things."""

    # Chain metadata google sheet.
    CHAIN_METADATA_GSHEET = "raw_gsheet_v1"

    # Across bridge metadat google sheet.
    ACROSS_BRIDGE_GSHEET = "raw_accross_bridge_gsheet_v1"

    # A dimension table storing metadata for ERC-20 tokens.
    # The metadata is fetched using RPC calls.
    ERC20_TOKEN_METADATA = "erc20_token_metadata_v1"

    # A dimension table that pulls the token list from the ethereum-optimism repo.
    SUPERCHAIN_TOKEN_LIST = "superchain_token_list_v1"
