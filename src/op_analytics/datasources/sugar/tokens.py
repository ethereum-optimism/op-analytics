from op_analytics.coreutils.logger import structlog
import polars as pl

from op_analytics.datasources.velodrome.sugarwrapper import fetch_pools, chain_cls_to_str

log = structlog.get_logger()

TOKEN_SCHEMA = {
    "chain": pl.Utf8,
    "token_address": pl.Utf8,
    "symbol": pl.Utf8,
    "decimals": pl.Int64,
    "listed": pl.Boolean,
}


def fetch_tokens_for_chain(chain_cls: type) -> pl.DataFrame:
    """
    Fetch token metadata for a single chain by leveraging sugarwrapper.py's fetch_pools().
    Returns a Polars DataFrame matching TOKEN_SCHEMA.
    """
    chain_str = chain_cls_to_str(chain_cls)
    log.info(f"Fetching tokens for {chain_str} via sugarwrapper fetch_pools()")

    # fetch_pools() returns a VelodromePools dataclass; we only need tokens here
    velodrome_pools = fetch_pools(chain_str)
    tokens = velodrome_pools.tokens  # a list of sugar.token.Token objects

    token_records = []
    for t in tokens:
        token_records.append(
            {
                "chain": chain_str,
                "token_address": t.token_address,
                "symbol": t.symbol,
                "decimals": t.decimals,
                "listed": t.listed,  # if the Token object has a 'listed' attribute
            }
        )

    df = pl.DataFrame(token_records, schema=TOKEN_SCHEMA)
    log.info(f"{chain_str} returned {df.height} tokens.")
    return df
