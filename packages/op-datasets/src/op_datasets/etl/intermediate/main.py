from op_coreutils.logger import bind_contextvars, clear_contextvars, structlog
from op_coreutils.partitioned import DataLocation
from op_coreutils.duckdb_inmem import init_client

from .construct import construct_tasks
from .registry import REGISTERED_INTERMEDIATE_MODELS, load_model_definitions
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
            **task.contextvars,
        )

        # Check output/input status for the task.
        checker(task)

        # Decide if we can run this task.
        if not task.inputdata.inputs_ready:
            log.warning("Task inputs are not ready. Skipping this task.")
            continue

        # Decide if we need to run this task.
        if task.data_writer.is_complete and not force:
            continue
        if force:
            log.info("Force flag detected. Forcing execution.")
            task.data_writer.force = True

        executor(task)

        writer(task)


def executor(task: IntermediateModelsTask):
    """Execute the model computations."""

    # Load shared DuckDB UDFs.
    client = init_client()
    create_duckdb_macros(client)

    for model in task.models:
        im_model = REGISTERED_INTERMEDIATE_MODELS[model]

        input_tables = {}
        for dataset in im_model.input_datasets:
            input_tables[dataset] = task.inputdata.duckdb_relation(dataset)

        for output_name, output in im_model.func(client, input_tables).items():
            task.add_output(output_name, output)

        produced_datasets = set(task.output_duckdb_relations.keys())
        if produced_datasets != set(im_model.expected_output_datasets):
            raise RuntimeError(f"model {model!r} produced unexpected datasets: {produced_datasets}")

    # Show the outputs that were produced by running the models.
    for key in task.output_duckdb_relations.keys():
        log.info(f"Task output: {key}")


def writer(task):
    """Write the model outputs"""

    # TODO: Implement writing.
    pass


def checker(task: IntermediateModelsTask):
    if task.data_writer.all_complete():
        task.data_writer.is_complete = True
        task.inputdata.inputs_ready = True
        return
