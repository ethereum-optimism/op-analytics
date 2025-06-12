from datetime import date

from op_analytics.coreutils.threads import run_concurrently_store_failures
from op_analytics.coreutils.time import now_date
from op_analytics.coreutils.logger import structlog

from .endpoints import find_system_configs
from .chainsystemconfig import ChainSystemConfig, SystemConfigList

log = structlog.get_logger()


def execute_pull(process_dt: date | None = None) -> SystemConfigList | None:
    """Pull and store system config data for all chains for the given date (or today)."""
    process_dt = process_dt or now_date()
    chains: list[ChainSystemConfig] = find_system_configs()
    targets: dict[str, ChainSystemConfig] = {_.identifier: _ for _ in chains}
    # Use lambda for fetch
    run_results = run_concurrently_store_failures(
        function=lambda x: x.fetch(process_dt=process_dt),
        targets=targets,
        max_workers=3,  # Reduced from 8 to 3 to avoid rate limiting
    )
    successful_data = [result for result in run_results.results.values() if result is not None]
    log.info(
        f"System config pull completed: {len(successful_data)} successful, {len(run_results.failures)} failed (out of {len(targets)} total)"
    )
    processed_data = SystemConfigList.store_system_config_data(successful_data)

    return processed_data
