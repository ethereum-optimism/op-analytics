from datetime import date


from op_analytics.coreutils.threads import run_concurrently
from op_analytics.coreutils.time import now_date
from op_analytics.coreutils.logger import structlog


from .database import find_tokens
from .chaintokens import ChainTokens


log = structlog.get_logger()


def execute_pull(process_dt: date | None = None):
    process_dt = process_dt or now_date()

    # Find the list of ERC20 tokens to update. These are grouped by chain.
    chains: list[ChainTokens] = find_tokens(process_dt=process_dt)
    targets: dict[str, ChainTokens] = {_.chain: _ for _ in chains}

    def _fetch(x: ChainTokens):
        return x.fetch(process_dt=process_dt)

    # Run each chain on a separate thread.
    results = run_concurrently(
        function=_fetch,
        targets=targets,
        max_workers=16,
    )

    return results
