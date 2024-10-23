from op_coreutils.logger import bind_contextvars, clear_contextvars, structlog

from .registry import REGISTERED_INTERMEDIATE_MODELS
from .task import IntermediateModelsTask
from .construct import construct_tasks


log = structlog.get_logger()


def compute_intermediate(
    chains: list[str],
    models: list[str],
    range_spec: str,
    source_spec: str,
    sinks_spec: list[str],
    dryrun: bool,
    force: bool = False,
):
    clear_contextvars()

    tasks = construct_tasks(chains, models, range_spec, source_spec, sinks_spec)

    for task in tasks:
        bind_contextvars(chain=task.chain, block=f"@{task.dt}")

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
