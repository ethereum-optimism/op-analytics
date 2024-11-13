import multiprocessing as mp

import polars as pl
from op_coreutils.logger import bind_contextvars, clear_contextvars, human_interval, structlog
from op_coreutils.partitioned import (
    DataLocation,
    OutputData,
)

from op_datasets.schemas import ONCHAIN_CURRENT_VERSION

from .audits import REGISTERED_AUDITS
from .construct import construct_tasks
from .markers import INGESTION_DATASETS
from .sources import RawOnchainDataProvider, read_from_source
from .status import all_inputs_ready
from .task import IngestionTask

log = structlog.get_logger()


def ingest(
    chains: list[str],
    range_spec: str,
    read_from: RawOnchainDataProvider,
    write_to: list[DataLocation],
    dryrun: bool,
    force: bool = False,
    fork_process: bool = True,
    max_tasks: int | None = None,
):
    clear_contextvars()

    tasks = construct_tasks(chains, range_spec, read_from, write_to)
    log.info(f"Constructed {len(tasks)} tasks.")

    if dryrun:
        log.info("DRYRUN: No work will be done.")
        return

    executed = 0
    executed_ok = 0
    for i, task in enumerate(tasks):
        task.progress_indicator = f"{i+1}/{len(tasks)}"
        bind_contextvars(**task.contextvars)

        # Check output/input status for the task.
        checker(task)

        # Decide if we can run this task.
        if not task.inputs_ready:
            log.warning("Task inputs are not ready. Skipping this task.")
            continue

        # Decide if we need to run this task.
        if task.data_writer.is_complete and not force:
            continue
        if force:
            log.info("Force flag detected. Forcing execution.")
            task.data_writer.force = True

        executed += 1
        success = execute(task, fork_process)
        executed_ok += 1 if success else 0

        if max_tasks is not None and executed >= max_tasks:
            log.warning(f"Stopping after executing {executed} tasks")
            break

    log.info(f"Execuded {executed} tasks. {executed_ok} succeeded.")


def execute(task, fork_process: bool) -> bool:
    """Returns true if task succeeds."""
    if fork_process:
        ctx = mp.get_context("spawn")
        p = ctx.Process(target=steps, args=(task,))
        p.start()
        p.join()

        if p.exitcode != 0:
            log.error(f"Process terminated with exit code: {p.exitcode}")
            return False
        else:
            log.info("Process terminated successfully.")
            return True
    else:
        steps(task)
        return True


def steps(task):
    bind_contextvars(**task.contextvars)

    # Read the data (updates the task in-place with the input dataframes).
    reader(task)

    # Run audits (updates the task in-pace with the output dataframes).
    auditor(task)

    # Write outputs and markers.
    writer(task)


def reader(task: IngestionTask):
    """Read core datasets from the specified source."""
    dataframes = read_from_source(
        provider=task.read_from,
        datasets=ONCHAIN_CURRENT_VERSION,
        block_batch=task.block_batch,
    )

    assert set(ONCHAIN_CURRENT_VERSION.keys()) == set(INGESTION_DATASETS)
    task.add_inputs(ONCHAIN_CURRENT_VERSION, dataframes)


def auditor(task: IngestionTask):
    """Run the audit process."""
    num_blocks = task.block_batch.max - task.block_batch.min

    num_seconds = (
        task.input_dataframes["blocks"]
        .select(pl.col("timestamp").max() - pl.col("timestamp").min())
        .item()
    )

    log.info(
        f"Auditing {num_blocks} {task.chain!r} blocks spanning {human_interval(num_seconds)} starting at block={task.block_batch.min}"
    )

    # Iterate over all the registered audits.
    # Raises an exception if an audit is failing.
    passing_audits = 0
    for name, audit in REGISTERED_AUDITS.items():
        # Execute the audit!
        result: pl.DataFrame = audit(task.input_dataframes)

        if not result.collect_schema().get("audit_name") == pl.String:
            raise Exception("Audit result DataFrame is missing column: audit_name[String]")

        if not result.collect_schema().get("failure_count") == pl.UInt32:
            raise Exception("Audit result DataFrame is missing column: failure_count[UInt32]")

        for audit_result in result.to_dicts():
            name = audit_result["audit_name"]
            value = audit_result["failure_count"]

            if value > 0:
                msg = f"audit failed: {name}"
                log.error(msg)
                raise Exception(msg)
            else:
                passing_audits += 1

    log.info(f"PASS {passing_audits} audits.")

    # Default values for "chain" and "dt" to be used in cases where one of the
    # other datsets is empty.  On chains with very low throughput (e.g. race) we
    # sometimes see no logs for a range of blocks. We still need to create a
    # marker for these empty dataframes.
    default_partition = (
        task.input_dataframes["blocks"].sort("number").select("chain", "dt").limit(1).to_dicts()[0]
    )

    # Set up the output dataframes now that the audits have passed
    # (ingestion process: outputs are the same as inputs)
    for name, dataset in task.input_datasets.items():
        task.store_output(
            OutputData(
                dataframe=task.input_dataframes[name],
                dataset_name=name,
                default_partition=default_partition,
            )
        )


def writer(task: IngestionTask):
    task.data_writer.write_all(outputs=task.output_dataframes)


def checker(task: IngestionTask):
    if task.data_writer.all_complete():
        task.data_writer.is_complete = True
        task.inputs_ready = True
        return

    if all_inputs_ready(
        provider=task.read_from,
        block_batch=task.block_batch,
        max_requested_timestamp=task.max_requested_timestamp,
    ):
        task.inputs_ready = True
        return
