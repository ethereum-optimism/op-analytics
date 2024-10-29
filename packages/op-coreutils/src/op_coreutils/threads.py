import concurrent.futures
from typing import Any, Callable
from op_coreutils.logger import structlog

log = structlog.get_logger()


def run_concurrently(
    function: Callable,
    targets: dict[str, Any] | list[str],
    max_workers: int | None = None,
) -> dict[str, Any]:
    """Concurrently call function on the provided targets.

    "targets" is a dictionary from key to function parameters. The key is used to identify the result in
    the results dictionary.
    """

    max_workers = max_workers or 4
    results = {}

    if isinstance(targets, list):
        targets = {k: k for k in targets}

    if max_workers == -1:
        return run_serially(function, targets)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}

        for key, target in targets.items():
            future = executor.submit(function, target)
            futures[future] = key

        for future in concurrent.futures.as_completed(futures):
            key = futures[future]
            try:
                results[key] = future.result()
            except Exception:
                log.error(f"Failed to run thread for {key}")
                raise

    return results


def run_serially(function: Callable, targets: dict[str, Any]) -> dict[str, Any]:
    results = {}
    for key, target in targets.items():
        results[key] = function(target)
    return results
