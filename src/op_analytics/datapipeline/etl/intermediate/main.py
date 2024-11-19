from op_analytics.coreutils.duckdb_inmem import init_client
from op_analytics.coreutils.logger import (
    bind_contextvars,
    clear_contextvars,
    structlog,
    bound_contextvars,
)
from op_analytics.coreutils.partitioned import DataLocation, OutputData

from .construct import construct_tasks
from .registry import REGISTERED_INTERMEDIATE_MODELS, load_model_definitions, PythonModelExecutor
from .task import IntermediateModelsTask
from .udfs import create_duckdb_macros

log = structlog.get_logger()


def compute_intermediate(
    chains: list[str],
    models: list[str],
    range_spec: str,
    read_from: DataLocation,
    write_to: list[DataLocation],
    dryrun: bool,
    force: bool = False,
):
    clear_contextvars()

    # Load python functions that define registered data models.
    load_model_definitions()

    for model in models:
        should_exit = False
        if model not in REGISTERED_INTERMEDIATE_MODELS:
            should_exit = True
            log.error("Model is not registered: {model}")
        if should_exit:
            log.error("Cannot run on unregistered models. Will exit.")
            exit(1)

    tasks = construct_tasks(chains, models, range_spec, read_from, write_to)

    if dryrun:
        log.info("DRYRUN: No work will be done.")
        return

    for i, task in enumerate(tasks):
        bind_contextvars(
            task=f"{i+1}/{len(tasks)}",
            **task.data_reader.contextvars,
        )

        # Check output/input status for the task.
        checker(task)

        # Decide if we can run this task.
        if not task.data_reader.inputs_ready:
            log.warning("Task inputs are not ready. Skipping this task.")
            continue

        # Decide if we need to run this task.
        if task.data_writer.is_complete and not force:
            continue
        if force:
            log.info("forced execution despite complete marker")
            task.data_writer.force = True

        executor(task)


def executor(task: IntermediateModelsTask) -> None:
    """Execute the model computations."""

    # Load shared DuckDB UDFs.
    client = init_client()
    create_duckdb_macros(client)

    for model_name in task.models:
        # Get the model.
        im_model = REGISTERED_INTERMEDIATE_MODELS[model_name]

        with PythonModelExecutor(im_model, client, task.data_reader) as m:
            with bound_contextvars(model=model_name):
                log.info("running model")
                model_results = m.execute()

                produced_datasets = set(model_results.keys())
                if produced_datasets != set(im_model.expected_output_datasets):
                    raise RuntimeError(
                        f"model {model_name!r} produced unexpected datasets: {produced_datasets}"
                    )

                for result_name, rel in model_results.items():
                    for location in task.data_writer.write_to:
                        log.info("writing model", result=result_name, location=location)
                        task.data_writer.write(
                            location=location,
                            output_data=OutputData(
                                dataframe=rel.pl(),
                                dataset_name=f"{model_name}/{result_name}",
                                default_partition={
                                    "chain": task.data_reader.chain,
                                    "dt": task.data_reader.datestr,
                                },
                            ),
                        )


def checker(task: IntermediateModelsTask) -> None:
    if task.data_writer.all_complete():
        task.data_writer.is_complete = True
        task.data_reader.inputs_ready = True
        return
