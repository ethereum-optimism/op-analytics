import polars as pl
from op_coreutils.logger import clear_contextvars, human_interval, structlog
from op_coreutils.storage.paths import Marker, PartitionedOutput, breakout_partitions

from op_datasets.coretables.sources import CoreDatasetSource
from op_datasets.pipeline.blockrange import BlockRange
from op_datasets.pipeline.ozone import (
    BlockBatch,
    IngestionTask,
    OutputDataFrame,
    split_block_range,
)
from op_datasets.pipeline.sinks import DataSink, IngestionProcessMarker, all_outputs_complete
from op_datasets.schemas import ONCHAIN_CURRENT_VERSION

log = structlog.get_logger()


def ingest(
    chains: list[str],
    block_spec: str,
    source_spec: str,
    sinks_spec: list[str],
    dryrun: bool,
    force: bool = False,
):
    clear_contextvars()

    tasks = construct_tasks(chains, block_spec)

    if dryrun:
        return

    sinks = construct_sinks(sinks_spec)

    for task in tasks:
        # Check and decide if we need to run this task.
        checker(task, sinks)
        if task.is_complete and not force:
            continue
        if force:
            log.info(f"Forcing execution of {task.pretty}")
            task.force = True

        # Read the data (updates the task in-place with the input dataframes).
        reader(task, source_spec)

        # Run audits (updates the task in-pace with the output dataframes).
        auditor(task)

        # Write outputs and markers.
        writer(task, sinks)


def construct_sinks(
    sinks_spec: list[str],
) -> list[DataSink]:
    sinks = []
    for sink_spec in sinks_spec:
        if sink_spec == "local":
            # Use the canonical location in our repo
            sinks.append(DataSink(sink_spec="file://ozone/warehouse/"))
        elif sink_spec == "gcs":
            sinks.append(DataSink(sink_spec=sink_spec))
        else:
            raise NotImplementedError(f"sink_spec not supported: {sink_spec}")
    return sinks


def construct_tasks(chains: list[str], block_spec: str):
    block_range = BlockRange.from_spec(block_spec)

    # Batches to be ingested for each chain.
    chain_batches: dict[str, list[BlockBatch]] = {}
    for chain in chains:
        chain_batches[chain] = split_block_range(chain, block_range)

    # Log a summayr of the work that will be done for each chain.
    for chain, batches in chain_batches.items():
        total_blocks = batches[-1].max - batches[0].min
        log.info(
            f"Will process chain={chain!r} {len(batches)} batch(es) {total_blocks} total blocks starting at #{batches[0].min}"
        )

    # Collect a single list of tasks to perform across all chains.
    all_tasks: list[IngestionTask] = []
    for batches in chain_batches.values():
        for batch in batches:
            all_tasks.append(IngestionTask.new(batch))

    return all_tasks


def reader(task: IngestionTask, source_spec: str, dataset_names: list[str] | None = None):
    """Read core datasets from the specified source."""
    log.info(f"Reading input data from {source_spec!r} for {task.pretty}")
    datasource = CoreDatasetSource.from_spec(source_spec)

    if dataset_names is not None:
        datasets = {}
        for name in dataset_names:
            datasets[name] = ONCHAIN_CURRENT_VERSION[name]
    else:
        datasets = ONCHAIN_CURRENT_VERSION

    dataframes = datasource.read_from_source(
        datasets=ONCHAIN_CURRENT_VERSION,
        block_batch=task.block_batch,
    )

    task.add_inputs(ONCHAIN_CURRENT_VERSION, dataframes)


def auditor(task: IngestionTask):
    num_blocks = task.block_batch.max - task.block_batch.min

    num_seconds = (
        task.input_dataframes["blocks"]
        .select(pl.col("timestamp").max() - pl.col("timestamp").min())
        .item()
    )

    log.info(
        f"Auditing {num_blocks} {task.chain!r} blocks spanning {human_interval(num_seconds)} starting at block={task.block_batch.min}"
    )

    # Run the audit process.
    from op_datasets.logic.audits.basic import REGISTERED_AUDITS

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


def writer(task: IngestionTask, sinks: list[DataSink]):
    for sink in sinks:
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
                marker=IngestionProcessMarker(
                    process_name="default",
                    chain=task.chain,
                    marker=Marker(
                        marker_path=output.marker_path,
                        outputs=written_parts,
                    ),
                )
            )


def checker(task: IngestionTask, sinks: list[DataSink]):
    if all_outputs_complete(sinks, task.expected_markers):
        log.info(
            f"{len(task.expected_markers)} outputs are already complete for #{task.block_batch.min}"
        )
        task.is_complete = True
