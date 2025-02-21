from typing import Any, Dict, List, Tuple

from op_analytics.coreutils.logger import structlog
from op_analytics.datasources.sugar.prices.dynamic_prices import fetch_prices_with_retry

log = structlog.get_logger()


async def fetch_chain_data(
    chain_cls: type,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Fetch chain data (tokens, pools, prices) for a given chain class.

    Args:
        chain_cls: A Sugar chain class (e.g. OPChain or BaseChain).

    Returns:
        A tuple with three lists:
          - tokens_data: List of dictionaries for tokens.
          - pools_data: List of dictionaries for liquidity pools.
          - prices_data: List of dictionaries for token prices.
    """
    # Initialize chain instance (assumed async context manager)
    async with chain_cls() as chain:
        tokens = await chain.get_all_tokens(listed_only=True)
        log.info(f"{chain_cls.__name__}: Fetched {len(tokens)} tokens.")

        # Build token mapping if needed
        tokens_data = [
            {
                "token_address": t.token_address,
                "symbol": t.symbol,
                "decimals": t.decimals,
                "listed": t.listed,
            }
            for t in tokens
        ]

        pools = await chain.get_pools()
        log.info(f"{chain_cls.__name__}: Fetched {len(pools)} liquidity pools.")
        pools_data = [
            {
                "lp": p.lp,
                "factory": p.factory,
                "symbol": p.symbol,
                "is_stable": p.is_stable,
                "total_supply": p.total_supply,
                "decimals": p.decimals,
                "token0": p.token0.symbol if p.token0 else None,
                "token1": p.token1.symbol if p.token1 else None,
                "pool_fee": p.pool_fee,
            }
            for p in pools
        ]

        prices_data = await fetch_prices_with_retry(chain, tokens, initial_batch_size=40)
        log.info(f"{chain_cls.__name__}: Fetched prices for {len(prices_data)} tokens.")

    return tokens_data, pools_data, prices_data
