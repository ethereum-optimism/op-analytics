import concurrent.futures
import math
from dataclasses import dataclass
from typing import Any, Callable

from op_analytics.coreutils.logger import bound_contextvars, human_interval, structlog
from op_analytics.coreutils.time import now

log = structlog.get_logger()


class ProgressTracker:
    """Counter and ETA tracking when running tasks concurrently.

    Wraps the user provided task function with bound context vars so that all
    logs include counter and ETA infor.
    """

    def __init__(self, total_tasks: int):
        self.total_tasks = total_tasks
        self.completed_tasks = 0
        self.start_time = now()

    def counter(self, current_index: int) -> str:
        return f"{current_index:03d}/{self.total_tasks:03d}"

    def eta(self) -> str | None:
        if self.completed_tasks < 3:
            return None

        elapsed = (now() - self.start_time).total_seconds()
        seconds_per_task = elapsed / self.completed_tasks
        remaining_tasks = self.total_tasks - self.completed_tasks
        eta_seconds = math.ceil(remaining_tasks * seconds_per_task)
        return human_interval(eta_seconds)

    def wrap(self, func, current_index: int):
        def wrapper(target_item):
            with bound_contextvars(counter=self.counter(current_index), eta=self.eta()):
                try:
                    return func(target_item)
                finally:
                    self.completed_tasks += 1

        return wrapper


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
