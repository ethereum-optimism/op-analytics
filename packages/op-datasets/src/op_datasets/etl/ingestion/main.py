import polars as pl
from op_coreutils.logger import bind_contextvars, clear_contextvars, human_interval, structlog
from op_coreutils.partitioned import (
    DataLocation,
    Marker,
    WrittenParquetPath,
    all_outputs_complete,
    breakout_partitions,
    marker_exists,
    write_marker,
    write_single_part,
)

from op_datasets.schemas import ONCHAIN_CURRENT_VERSION

from .audits import REGISTERED_AUDITS
from .construct import construct_tasks
from .markers import IngestionCompletionMarker
from .sources import RawOnchainDataProvider, read_from_source
from .status import all_inputs_ready
from .task import IngestionTask, OutputDataFrame

log = structlog.get_logger()


def ingest(
    chains: list[str],
    range_spec: str,
    read_from: RawOnchainDataProvider,
    write_to: list[DataLocation],
    dryrun: bool,
    force: bool = False,
):
    clear_contextvars()

    tasks = construct_tasks(chains, range_spec, read_from, write_to)
    log.info(f"Constructed {len(tasks)} tasks.")

    if dryrun:
        log.info("DRYRUN: No work will be done.")
        return

    not_skipped = 0
    for i, task in enumerate(tasks):
        bind_contextvars(
            task=f"{i+1}/{len(tasks)}",
            **task.contextvars,
        )

        # Check and decide if we need to run this task.
        checker(task)
        if not task.inputs_ready:
            log.warning("Task inputs are not ready. Skipping this task.")
            continue
        if task.is_complete and not force:
            continue
        if force:
            log.info("Force flag detected. Forcing execution.")
            task.force = True

        not_skipped += 1

        # Read the data (updates the task in-place with the input dataframes).
        reader(task)

        # Run audits (updates the task in-pace with the output dataframes).
        auditor(task)

        # Write outputs and markers.
        writer(task)

        if not_skipped > 20:
            log.warning(f"Stopping after executing {not_skipped} tasks")
            break


def reader(task: IngestionTask):
    """Read core datasets from the specified source."""
    dataframes = read_from_source(
        provider=task.read_from,
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
                dataset_name=name,
            )
        )


def writer(task: IngestionTask):
    for location in task.write_to:
        for output in task.output_dataframes:
            is_complete = marker_exists(location, output.marker_path)

            if is_complete and not task.force:
                log.info(
                    f"[{location.name}] Skipping already complete output at {output.marker_path}"
                )
                continue

            written_parts: list[WrittenParquetPath] = []

            parts = breakout_partitions(
                df=output.dataframe,
                partition_cols=["chain", "dt"],
                root_path=output.root_path,
                basename=task.block_batch.construct_parquet_filename(),
            )

            for part_df, part in parts:
                write_single_part(
                    location=location,
                    dataframe=part_df,
                    part_output=part,
                )
                written_parts.append(part)

            # TODO: Generalize Marker so it accepts additional columns
            marker = IngestionCompletionMarker(
                num_blocks=task.block_batch.num_blocks(),
                min_block=task.block_batch.min,
                max_block=task.block_batch.max,
                chain=task.chain,
                marker=Marker(
                    marker_path=output.marker_path,
                    dataset_name=output.dataset_name,
                    data_paths=written_parts,
                    chain=task.block_batch.chain,
                    process_name="default",
                ),
            )

            write_marker(
                location=location,
                arrow_table=marker.to_pyarrow_table(),
            )


def checker(task: IngestionTask):
    if all_outputs_complete(task.write_to, task.expected_markers):
        task.is_complete = True
        task.inputs_ready = True
        return

    if all_inputs_ready(task.read_from, task.block_batch):
        task.inputs_ready = True
        return
