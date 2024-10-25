from op_coreutils.logger import bind_contextvars, clear_contextvars, structlog

from op_datasets.etl.ingestion.sinks import RawOnchainDataLocation
from op_datasets.etl.ingestion.status import all_outputs_complete
from .registry import REGISTERED_INTERMEDIATE_MODELS
from .task import IntermediateModelsTask
from .construct import construct_tasks

log = structlog.get_logger()


def compute_intermediate(
    chains: list[str],
    models: list[str],
    range_spec: str,
    read_from: RawOnchainDataLocation,
    write_to: list[RawOnchainDataLocation],
    dryrun: bool,
    force: bool = False,
):
    clear_contextvars()

    tasks = construct_tasks(chains, models, range_spec, read_from, write_to)
    log.info(f"Constructed {len(tasks)} tasks.")

    if dryrun:
        log.info("DRYRUN: No work will be done.")
        return

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

        executor(task)

        writer(task)


def executor(task: IntermediateModelsTask):
    """Execute the model computations."""

    for model in task.models:
        model_func = REGISTERED_INTERMEDIATE_MODELS[model]

        for name, output in model_func(task).items():
            task.add_output(name, output)

    # Show the outputs that were produced by running the models.
    for key in task.output_duckdb_relations.keys():
        log.info(f"Task output: {key}")


def writer(task):
    """Write the model outputs"""

    # TODO: Implement writing.
    pass


def checker(task: IntermediateModelsTask):
    if all_outputs_complete(task.write_to, task.expected_markers):
        task.is_complete = True
        return
