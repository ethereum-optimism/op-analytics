from datetime import date

from op_analytics.coreutils.threads import run_concurrently_store_failures
from op_analytics.coreutils.time import now_date
from op_analytics.coreutils.logger import structlog

from .endpoints import find_system_configs
from .chainsystemconfig import ChainSystemConfig

log = structlog.get_logger()


def execute_pull(process_dt: date | None = None):
    process_dt = process_dt or now_date()

    # Find the list of chains with system configs to update.
    chains: list[ChainSystemConfig] = find_system_configs()
    targets: dict[str, ChainSystemConfig] = {_.identifier: _ for _ in chains}

    def _fetch(x: ChainSystemConfig):
        return x.fetch(process_dt=process_dt)

    # Run each chain on a separate thread.
    run_results = run_concurrently_store_failures(
        function=_fetch,
        targets=targets,
        max_workers=8,
    )

    log.info(
        f"System config pull completed: {len(run_results.results)} successful, {len(run_results.failures)} failed"
    )

    # Return just the successful results (matching the erc20tokens pattern)
    return run_results.results
