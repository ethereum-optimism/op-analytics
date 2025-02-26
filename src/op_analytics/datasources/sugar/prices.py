from op_analytics.coreutils.logger import structlog
import polars as pl

from sugar.price import Price

from op_analytics.datasources.velodrome.sugarwrapper import fetch_pools, chain_cls_to_str

log = structlog.get_logger()

PRICES_SCHEMA = {
    "chain": pl.Utf8,
    "token_address": pl.Utf8,
    "price": pl.Float64,
}


def fetch_prices_for_chain(chain_cls: type) -> pl.DataFrame:
    """
    Fetch token prices for each chain by leveraging sugarwrapper.py's fetch_pools().
    We then build a DataFrame from the returned Price objects.
    """
    chain_str = chain_cls_to_str(chain_cls)
    log.info(f"Fetching prices for {chain_str} via sugarwrapper fetch_pools()")

    velodrome_pools = fetch_pools(chain_str)
    prices = velodrome_pools.prices  # list[Price]

    price_records = []
    for cp in prices:
        if not isinstance(cp, Price):
            continue
        price_records.append(
            {
                "chain": chain_str,
                "token_address": cp.token.token_address,
                "price": cp.price,
            }
        )

    df = pl.DataFrame(price_records, schema=PRICES_SCHEMA)
    log.info(f"{chain_str} returned {df.height} prices.")
    return df
