import polars as pl

from op_analytics.coreutils.logger import structlog


log = structlog.get_logger()

URL_BASE = (
    "https://raw.githubusercontent.com/ethereum-optimism/superchain-registry/refs/heads/main/"
)
SUPERCHAIN_CHAIN_LIST = "chainList.json"

SUPERCHAIN_CHAIN_LIST_SCHEMA = pl.Schema(
    {
        "name": pl.String,
        "identifier": pl.String,
        "chain_id": pl.Int32,
        "rpc": pl.List(pl.String),
        "explorers": pl.List(pl.String),
        "superchain_level": pl.Int32,
        "governed_by_optimism": pl.Boolean,
        "data_availability_type": pl.String,
        "parent_type": pl.String,
        "parent_chain": pl.String,
        "gas_paying_token": pl.String,
        "fault_proofs_status": pl.String,
    }
)
