import importlib
import os
from op_coreutils.logger import bind_contextvars, clear_contextvars, structlog

from .models import REGISTERED_INTERMEDIATE_MODELS
from .task import construct_tasks, IntermediateModelsTask


def find_modules(path: str):
    """Finds all python modules that define models in the path.

    This is used to auto-discover models based on convention.
    """
    modules = []

    for basename in os.listdir(path):
        name = os.path.join(path, basename)
        if os.path.isfile(name) and basename not in ("__init__.py", "registry.py"):
            modules.append(basename.removesuffix(".py"))

    return sorted(modules)


# Python modules under the "models" directory are imported to populate the model registry.
MODELS_PATH = os.path.join(os.path.dirname(__file__), "models")
for basename in os.listdir(MODELS_PATH):
    name = os.path.join(MODELS_PATH, basename)
    if os.path.isfile(name) and basename not in ("__init__.py", "registry.py"):
        importlib.import_module(
            f"op_datasets.etl.intermediate.models.{basename.removesuffix(".py")}"
        )


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
