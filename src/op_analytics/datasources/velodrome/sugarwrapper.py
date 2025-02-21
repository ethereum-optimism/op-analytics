import itertools
from dataclasses import dataclass

from sugar.chains import BaseChain, OPChain, Chain
from sugar.price import Price
from sugar.token import Token
from sugar.pool import LiquidityPool
from sugar.helpers import normalize_address

from op_analytics.coreutils.coroutines import run_coroutine
from op_analytics.coreutils.logger import structlog


log = structlog.get_logger()


# Map our chain names to the sugar sdk Chain class.
SUGAR_CHAINS = {
    "op": OPChain(),
    "base": BaseChain(),
}


@dataclass(frozen=True)
class MissingTokenInfo:
    """A LiquidityPool that could not be instantiated due to missing token information."""

    token0: str
    token1: str
    pool_type: str


@dataclass
class VelodromePools:
    """A class to store the results from scraping Velodrome pools using the sugar contracts."""

    tokens: list[Token]
    prices: list[Price]
    pools: list[LiquidityPool]

    def show(self):
        tokens_str = "\n".join(str(_) for _ in self.tokens[:5])
        prices_str = "\n".join(str(_) for _ in self.prices[:5])
        pools_str = "\n".join(str(_) for _ in self.pools[:5])

        summary = "\n".join(
            [
                f"--- TOKENS ({len(self.tokens)}) ---",
                tokens_str,
                f"\n--- PRICES ({len(self.prices)})--- ",
                prices_str,
                f"\n--- POOLS({len(self.pools)}) --- ",
                pools_str,
            ]
        )
        print(summary)


def fetch_pools(chain: str) -> VelodromePools:
    """Wrap the Sugar SDK to fetch Velodrome pools data."""

    sugar_chain: Chain = SUGAR_CHAINS[chain]

    async def _coro():
        async with sugar_chain as chain_ctx:
            chain_pools = await _sugar_pools(chain, chain_ctx)
        return chain_pools

    return run_coroutine(_coro())


async def _sugar_pools(chain: str, sugar_chain: Chain) -> VelodromePools:
    """Leverage the sugar sdk to collect VelodromePools data.

    This function should not be used outside of this module.
    Use the fetch_pools sync wrapper instead.
    """

    tokens = await sugar_chain.get_all_tokens(listed_only=False)
    log.info(f"chain={chain} fetched {len(tokens)} tokens")

    # (pedrod - 2025/02/21) This was copied over from the sugar SDK.
    # filter out stable token from tokens list so getManyRatesWithCustomConnectors so does not freak out
    stable_address = sugar_chain.settings.stable_token_addr
    tokens_without_stable = list(filter(lambda t: t.token_address != stable_address, tokens))
    stable = next(filter(lambda t: t.token_address == stable_address, tokens), None)

    prices = []
    if stable is not None:
        prices.append(Price(token=stable, price=1))

    # Do not use asyncio gather otherwise we risk getting rate limited by the RPC.
    batches = list(itertools.batched(tokens_without_stable, n=10))
    for ii, batch in enumerate(batches):
        batch_prices = await sugar_chain._get_prices(tuple(batch))
        log.info(
            f"{chain=} fetched {len(batch_prices)} prices from batch {ii+1:03d}/{len(batches):03d}"
        )
        prices.extend(batch_prices)
    log.info(f"{chain=} done fetching {len(prices)} prices")

    # Paginate to fetch information for all liquidity pools. Stores raw responses.
    pools = []
    iteration = 0
    offset = 0
    limit = 50  # page size
    while True:
        iteration += 1
        pools_batch = await sugar_chain.sugar.functions.all(limit, offset).call()
        log.info(f"{chain=} {iteration=} fetched {len(pools_batch)} pools")
        pools += pools_batch
        if len(pools_batch) < limit:
            break
        else:
            offset += limit
    log.info(f"{chain=} done fetching {len(pools)} pools")

    # Marshall the raw responses into LiquidityPool objects.
    tokens_index = {t.token_address: t for t in tokens}
    lps = []
    missing = []
    for pool in pools:
        lp = LiquidityPool.from_tuple(pool, tokens_index)

        if lp is None:
            missing.append(
                MissingTokenInfo(
                    token0=normalize_address(pool[7]),
                    token1=normalize_address(pool[10]),
                    pool_type=pool[4],
                )
            )
        else:
            lps.append(lp)

    if len(missing) > 0:
        raise Exception(f"possible error, token info not found for pools {missing}")

    # Pack all the responses into a VelodromePools object.
    return VelodromePools(
        tokens=tokens,
        prices=prices,
        pools=lps,
    )
