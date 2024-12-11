import concurrent.futures
from dataclasses import dataclass
from typing import Any, Callable

from op_analytics.coreutils.logger import structlog, ProgressTracker

log = structlog.get_logger()


def run_concurrently(
    function: Callable,
    targets: dict[str, Any] | list[Any],
    max_workers: int | None = None,
) -> dict[Any, Any]:
    """Concurrently call function on the provided targets.

    "targets" is a dictionary from key to function parameters. The key is used to identify the result in
    the results dictionary.

    Sometimes you have function that accepts arguments in addition to the function target
    that you need to run concurrently. For those cases we recommend that you create a
    single arg function wrapper at the call site which you can then pass to run_concurrently.
    """

    max_workers = max_workers or 4
    results = {}

    if isinstance(targets, list):
        targets = {k: k for k in targets}

    if max_workers == -1:
        return run_serially(function, targets)

    num_targets = len(targets)
    tracker = ProgressTracker(total_tasks=num_targets)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}

        for i, (key, target) in enumerate(targets.items()):
            task_callable = tracker.wrap(function, current_index=i + 1)
            future = executor.submit(task_callable, target)
            futures[future] = key

        for future in concurrent.futures.as_completed(futures):
            key = futures[future]
            try:
                results[key] = future.result()
            except Exception as ex:
                log.error(f"Failed to run thread for {key}", exc_info=ex)
                raise

    return results


def run_serially(function: Callable, targets: dict[str, Any]) -> dict[str, Any]:
    results = {}
    for key, target in targets.items():
        results[key] = function(target)
    return results


@dataclass
class RunResults:
    """Helper class for when we want to proceed despite failures."""

    results: dict[Any, Any]
    failures: dict[Any, str]


def run_concurrently_store_failures(
    function: Callable,
    targets: dict[str, Any] | list[Any],
    max_workers: int | None = None,
) -> RunResults:
    """Same as run_concurrently but do not fail if a target results in an Exception."""
    max_workers = max_workers or 4

    results = {}
    failures = {}

    if isinstance(targets, list):
        targets = {k: k for k in targets}

    if max_workers == -1:
        return run_serially_store_failures(function, targets)

    num_targets = len(targets)
    tracker = ProgressTracker(total_tasks=num_targets)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}

        for i, (key, target) in enumerate(targets.items()):
            task_callable = tracker.wrap(function, current_index=i + 1)
            future = executor.submit(task_callable, target)
            futures[future] = key

        for future in concurrent.futures.as_completed(futures):
            key = futures[future]
            try:
                results[key] = future.result()
            except Exception as ex:
                log.error(f"Failed to run thread for {key}", exc_info=ex)
                failures[key] = str(ex)

    return RunResults(results=results, failures=failures)


def run_serially_store_failures(function: Callable, targets: dict[str, Any]) -> RunResults:
    """Same as run_serially but do not fail if a target results in an Exception."""
    results = {}
    failures = {}
    for key, target in targets.items():
        try:
            results[key] = function(target)
        except Exception as ex:
            log.error(f"Failed to run {key}", exc_info=ex)
            failures[key] = str(ex)
    return RunResults(results=results, failures=failures)
