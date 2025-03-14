import multiprocessing as mp
from typing import Any, Callable, Iterable

from op_analytics.coreutils.logger import memory_usage, structlog

log = structlog.get_logger()


def run_in_subprocesses[T](
    target: Callable[[T], Any],
    tasks: Iterable[T],
    total_tasks: int,
    fork: bool = True,
):
    """Run the callable in a subprocess.

    Using subprocesses helps control memory usage for long running jobs.
    """
    for i, task in enumerate(tasks):
        log.info(f"running batch {i+1:03d}/{total_tasks:03d}", max_rss=memory_usage())
        if not fork:
            target(task)
        else:
            ctx = mp.get_context("spawn")
            p = ctx.Process(target=target, args=(task,))
            p.start()
            try:
                p.join()
            except KeyboardInterrupt:
                log.info("Keyboard interrupt received. Terminating spawned process")
                p.terminate()
                p.join()
                raise

            if p.exitcode != 0:
                raise Exception(f"forked process failed with exitcode={p.exitcode}")

            log.info("forked process success", max_rss=memory_usage())
