from typing import List

from sugar.pool import LiquidityPool
from op_analytics.coreutils.logger import structlog

log = structlog.get_logger()


async def fetch_pool_data(chain) -> List[LiquidityPool]:
    """
    Fetch raw pool data without calling get_prices. We build a token mapping
    from get_all_tokens directly, then map them into LiquidityPool objects.

    Handles pagination with retries on out-of-gas errors by reducing batch size.
    """
    pools = []
    offset = 0
    limit = chain.settings.pool_page_size

    tokens = await chain.get_all_tokens(listed_only=True)
    tokens_map = {t.token_address: t for t in tokens}

    while True:
        try:
            pools_batch = await chain.sugar.functions.all(limit, offset).call()
            pools.extend(pools_batch)
            log.info(
                "Fetched pool batch",
                offset=offset,
                batch_size=len(pools_batch),
                total_pools=len(pools),
            )
            if len(pools_batch) < limit:
                break
            offset += limit

        except Exception as exc:
            error_str = str(exc)
            if "out of gas" in error_str:
                if limit > 1:
                    new_limit = max(1, limit // 2)
                    log.warning(
                        "Reducing batch size due to out of gas error",
                        old_size=limit,
                        new_size=new_limit,
                    )
                    limit = new_limit
                else:
                    log.error("Failed to fetch pools with minimum batch size", error=error_str)
                    raise
            else:
                log.error("Unexpected error fetching pools", error=error_str)
                raise

    result = [LiquidityPool.from_tuple(p, tokens_map) for p in pools if p is not None]
    log.info("Pool data fetch completed", total_pools=len(result))
    return result
