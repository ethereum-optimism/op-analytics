from op_analytics.coreutils.logger import structlog
import polars as pl
from sugar.pool import LiquidityPool

from op_analytics.datasources.velodrome.sugarwrapper import fetch_pools
from op_analytics.datasources.velodrome.chain_list import chain_cls_to_str

log = structlog.get_logger()

POOLS_SCHEMA = {
    "chain": pl.Utf8,
    "lp": pl.Utf8,
    "factory": pl.Utf8,
    "symbol": pl.Utf8,
    "is_stable": pl.Boolean,
    "total_supply": pl.Float64,
    "decimals": pl.Int64,
    "token0": pl.Utf8,
    "token1": pl.Utf8,
    "pool_fee": pl.Float64,
    "gauge_total_supply": pl.Float64,
    "emissions_token": pl.Utf8,
    "nfpm": pl.Utf8,
    "alm": pl.Utf8,
}


def fetch_pools_for_chain(chain_cls: type) -> pl.DataFrame:
    """
    Fetch pool data from sugarwrapper.py. We then map the returned LiquidityPool objects
    into a Polars DataFrame defined by POOLS_SCHEMA.
    """
    chain_str = chain_cls_to_str(chain_cls)
    log.info(f"Fetching pools for {chain_str}")

    velodrome_pools = fetch_pools(chain_str)
    raw_pools = velodrome_pools.pools  # list[LiquidityPool]

    pool_records = []
    for lp in raw_pools:
        if not isinstance(lp, LiquidityPool):
            continue
        pool_records.append(
            {
                "chain": chain_str,
                "lp": lp.lp,
                "symbol": lp.symbol,
                "factory": lp.factory,
                "is_stable": lp.is_stable,
                "total_supply": float(lp.total_supply),
                "decimals": int(lp.decimals),
                "token0": lp.token0.token_address if lp.token0 else "unknown",
                "token1": lp.token1.token_address if lp.token1 else "unknown",
                "pool_fee": float(lp.pool_fee),
                "gauge_total_supply": float(lp.gauge_total_supply),
                "emissions_token": lp.emissions_token.token_address
                if lp.emissions_token
                else "unknown",
                "nfpm": lp.nfpm,
                "alm": lp.alm,
            }
        )

    df = pl.DataFrame(pool_records, schema=POOLS_SCHEMA)
    log.info(f"{chain_str} returned {df.height} pools.")
    return df
