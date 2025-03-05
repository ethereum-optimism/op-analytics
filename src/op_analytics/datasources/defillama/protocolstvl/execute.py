import itertools
import multiprocessing as mp
from datetime import date
from typing import Any, Callable, Iterable

from op_analytics.coreutils.logger import memory_usage, structlog
from op_analytics.coreutils.time import now_date

from ..dataaccess import DefiLlama
from .metadata import ProtocolMetadata
from .utils import Batch, copy_to_gcs, fetch_and_write, get_buffered

log = structlog.get_logger()


TVL_TABLE_LAST_N_DAYS = 360


def execute_pull(process_dt: date | None = None):
    """Daily pull protocol TVL data from DefiLlama."""

    process_dt = process_dt or now_date()

    # Fetch the list of protocols and their metadata
    metadata = ProtocolMetadata.fetch(process_dt)
    DefiLlama.PROTOCOLS_METADATA.write(
        dataframe=metadata.df,
        sort_by=["protocol_slug"],
    )

    # Create list of slugs to fetch protocol-specific data
    slugs = metadata.slugs()

    # Fetch from API And write to buffer.
    write_to_buffer(slugs, process_dt)

    # Copy data from buffer to GCS.
    return copy_to_gcs(process_dt=process_dt, last_n_days=TVL_TABLE_LAST_N_DAYS)


def write_to_buffer(slugs: list[str], process_dt: date):
    """Pull data from DefiLlama and write to the ClickHouse buffer.

    This part was split up as a a separate function just so it is easy to add retries
    to this part of the process only if we need to later on.
    """

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
    )

    log.info("done fetching and buffering data")


def batches(process_dt: date, pending_ids: list[str], n: int):
    """Split the pending ids into batches for processing."""

    for batch in itertools.batched(pending_ids, n=n):
        yield Batch(
            process_dt=process_dt,
            slugs=list(batch),
        )


def run_in_subprocesses[T](target: Callable[[T], Any], tasks: Iterable[T], total_tasks: int):
    """Run the callable in a subprocess.

    Using subprocesses helps control memory usage for long running jobs.
    """
    for i, task in enumerate(tasks):
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
