import polars as pl
from op_coreutils.logger import bind_contextvars, clear_contextvars, human_interval, structlog
from op_coreutils.storage.paths import Marker, PartitionedOutput, breakout_partitions

from op_datasets.etl.ingestion.sinks import (
    IngestionCompletionMarker,
    all_outputs_complete,
)
from op_datasets.schemas import ONCHAIN_CURRENT_VERSION

from .audits import REGISTERED_AUDITS
from .task import IngestionTask, OutputDataFrame, construct_tasks

log = structlog.get_logger()


def ingest(
    chains: list[str],
    range_spec: str,
    source_spec: str,
    sinks_spec: list[str],
    dryrun: bool,
    force: bool = False,
):
    clear_contextvars()

    tasks = construct_tasks(chains, range_spec, source_spec, sinks_spec)

    if dryrun:
        return

    for task in tasks:
        bind_contextvars(chain=task.block_batch.chain, block=f"#{task.block_batch.min}")
        # Check and decide if we need to run this task.
        checker(task)
        if task.is_complete and not force:
            continue
        if force:
            log.info(f"Forcing execution of {task.pretty}")
            task.force = True

        # Read the data (updates the task in-place with the input dataframes).
        reader(task)

        # Run audits (updates the task in-pace with the output dataframes).
        auditor(task)

        # Write outputs and markers.
        writer(task)


def reader(task: IngestionTask):
    """Read core datasets from the specified source."""
    log.info(f"Reading input data from for {task.pretty}")

    dataframes = task.source.read_from_source(
        datasets=ONCHAIN_CURRENT_VERSION,
        block_batch=task.block_batch,
    )

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

    # Set up the output dataframes now that the audits have passed
    for name, dataset in task.input_datasets.items():
        task.add_output(
            OutputDataFrame(
                dataframe=task.input_dataframes[name],
                root_path=task.get_output_location(dataset),
                marker_path=task.get_marker_location(dataset),
            )
        )


def writer(task: IngestionTask):
    for sink in task.sinks:
        for output in task.output_dataframes:
            if sink.is_complete(output.marker_path) and not task.force:
                log.info(
                    f"[{sink.sink_spec}] Skipping already complete output at {output.marker_path}"
                )
                continue

            written_parts: list[PartitionedOutput] = []

            parts = breakout_partitions(
                df=output.dataframe,
                partition_cols=["chain", "dt"],
                root_path=output.root_path,
                basename=task.block_batch.construct_parquet_filename(),
            )

            for part_df, part in parts:
                sink.write_single_part(dataframe=part_df, part_output=part)
                written_parts.append(part)

            sink.write_marker(
                marker=IngestionCompletionMarker(
                    process_name="default",
                    chain=task.chain,
                    marker=Marker(
                        marker_path=output.marker_path,
                        outputs=written_parts,
                    ),
                )
            )


def checker(task: IngestionTask):
    if all_outputs_complete(task.sinks, task.expected_markers):
        task.is_complete = True
