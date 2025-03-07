import itertools
import multiprocessing as mp
from datetime import date
from typing import Any, Callable, Iterable

from op_analytics.coreutils.logger import memory_usage, structlog

from .fetch import Batch, fetch_and_write, get_buffered

log = structlog.get_logger()


def write_to_buffer(slugs: list[str], process_dt: date):
    """Pull data from DefiLlama and write to the ClickHouse buffer."""

    # Find out which slugs are still pending.
    buffered_ids = get_buffered(process_dt=process_dt)
    pending_ids = list(set(slugs) - set(buffered_ids))

    # Fetch data and write to buffer for pending slugs.
    log.info(f"fetching and buffering data for {len(pending_ids)}/{len(slugs)} pending slugs")

    # Split into batches of "batch_size" slugs per batch.
    batch_size = 120
    num_pending = len(pending_ids)
    total_tasks = (num_pending // batch_size) + (1 if num_pending % batch_size > 0 else 0)

    # Loop over batches in subprocess to avoid memory usage creep. The subprocesses
    # return memory back to the os.
    run_in_subprocesses(
        target=fetch_and_write,
        tasks=batches(
            process_dt=process_dt,
            pending_ids=pending_ids,
            n=batch_size,
        ),
        total_tasks=total_tasks,
        fork=False,  # Set to False when debugging locally
    )

    log.info("done fetching and buffering data")


def batches(process_dt: date, pending_ids: list[str], n: int):
    """Split the pending ids into batches for processing."""

    for batch in itertools.batched(pending_ids, n=n):
        yield Batch(
            process_dt=process_dt,
            slugs=list(batch),
        )


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
        if not fork:
            target(task)
        else:
            log.info(f"running batch {i+1:03d}/{total_tasks:03d}", max_rss=memory_usage())
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
