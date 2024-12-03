import multiprocessing as mp
import resource
from collections import defaultdict

import polars as pl

from op_analytics.coreutils.logger import (
    bound_contextvars,
    human_interval,
    human_rows,
    structlog,
)
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.coreutils.partitioned.output import OutputData

from op_analytics.datapipeline.schemas import ONCHAIN_CURRENT_VERSION

from .audits import REGISTERED_AUDITS
from .construct import construct_tasks
from .markers import INGESTION_DATASETS
from .sources import RawOnchainDataProvider, read_from_source
from .status import all_inputs_ready
from .task import IngestionTask

log = structlog.get_logger()


@bound_contextvars(pipeline_step="ingest")
def ingest(
    chains: list[str],
    range_spec: str,
    read_from: RawOnchainDataProvider,
    write_to: DataLocation,
    dryrun: bool,
    force_complete: bool = False,
    fork_process: bool = True,
    max_tasks: int | None = None,
):
    tasks = construct_tasks(chains, range_spec, read_from, write_to)
    log.info(f"constructed {len(tasks)} tasks.")

    if dryrun:
        log.info("DRYRUN: No work will be done.")
        return

    executed = 0
    executed_ok = 0
    for i, task in enumerate(tasks):
        task.progress_indicator = f"{i+1}/{len(tasks)}"
        with bound_contextvars(**task.contextvars):
            # Decide if we need to run this task.
            if task.data_writer.is_complete() and not force_complete:
                log.info("task", status="already_complete")
                continue

            if force_complete:
                task.data_writer.force = True

            # Decide if we can run this task.
            if not all_inputs_ready(
                provider=task.read_from,
                block_batch=task.block_batch,
                max_requested_timestamp=task.max_requested_timestamp,
            ):
                log.warning("task", status="input_not_ready")
                continue

            executed += 1
            success = execute(task, fork_process)
            log.info("task", status="success" if success else "fail")
            executed_ok += 1 if success else 0

            if max_tasks is not None and executed >= max_tasks:
                log.warning(f"stopping after {executed} tasks")
                break


def execute(task, fork_process: bool) -> bool:
    """Returns true if task succeeds."""
    if fork_process:
        ctx = mp.get_context("spawn")
        p = ctx.Process(target=steps, args=(task,))
        p.start()
        p.join()

        max_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

        if p.exitcode != 0:
            log.error(f"task exit code: {p.exitcode}", maxrss=max_rss)
            return False
        else:
            log.info("task exit 0", maxrss=max_rss)
            return True
    else:
        steps(task)
        return True


def steps(task):
    with bound_contextvars(**task.contextvars):
        # Read the data (updates the task in-place with the input dataframes).
        reader(task)

        # Run audits (updates the task in-pace with the output dataframes).
        auditor(task)

        # Write outputs and markers.
        writer(task)


def reader(task: IngestionTask):
    """Read core datasets from the specified source."""
    log.info("querying core datasets")
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
        f"auditing {num_blocks} {task.chain!r} blocks spanning {human_interval(num_seconds)} starting at block={task.block_batch.min}"
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

    log.info(f"audit {passing_audits} checks OK")

    # Default values for "chain" and "dt" to be used in cases where one of the
    # other datsets is empty.  On chains with very low throughput (e.g. race) we
    # sometimes see no logs for a range of blocks. We still need to create a
    # marker for these empty dataframes.
    blocks_df = update_chain_name(task=task, df=task.input_dataframes["blocks"])
    default_partition = blocks_df.sort("number").select("chain", "dt").limit(1).to_dicts()[0]

    # Set up the output dataframes now that the audits have passed
    # (ingestion process: outputs are the same as inputs)
    for name, dataset in task.input_datasets.items():
        df = update_chain_name(task=task, df=task.input_dataframes[name])

        task.store_output(
            OutputData(
                dataframe=df,
                root_path=task.block_batch.dataset_directory(dataset_name=name),
                default_partition=default_partition,
            )
        )


def update_chain_name(task, df):
    # On testnet data we want the "chain" partition value to
    # include the "sepolia" suffix. This makes downstream easier
    # because with only the "chain" column we can disambiguate
    # mainnet and testnet.
    if task.is_testnet:
        if df.is_empty():
            expected = []
        else:
            expected = [task.chain_parent]
        chains_in_df = df["chain"].unique().to_list()
        assert chains_in_df == expected, f"{chains_in_df} != {expected}"
        return df.with_columns(chain=pl.lit(task.chain))

    else:
        return df


def writer(task: IngestionTask):
    total_rows: dict[str, int] = defaultdict(int)

    for output_data in task.output_dataframes:
        parts = task.data_writer.write(output_data)

        for part in parts:
            total_rows[output_data.root_path] += part.row_count

    summary = " ".join(f"{key}={human_rows(val)}" for key, val in total_rows.items())
    summary = f"{task.data_writer.write_to.name}::{summary}"
    log.info(f"done writing. {summary}")
