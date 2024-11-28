import importlib
import os

from op_analytics.coreutils.logger import structlog, bound_contextvars

from .modelexecute import PythonModel
from .querybuilder import TemplatedSQLQuery

log = structlog.get_logger()

# Flag to prevent reloading model definitions.
_LOADED = False


REGISTERED_INTERMEDIATE_MODELS: dict[str, PythonModel] = {}


def register_model(
    input_datasets: list[str],
    expected_outputs: list[str],
    duckdb_views: list[TemplatedSQLQuery],
):
    def decorator(func):
        model_name = func.__name__
        with bound_contextvars(model=model_name):
            rendered_views = []
            for q in duckdb_views or []:
                rendered = q.render()
                rendered_views.append(rendered)

            REGISTERED_INTERMEDIATE_MODELS[model_name] = PythonModel(
                name=model_name,
                input_datasets=input_datasets,
                expected_output_datasets=expected_outputs,
                duckdb_views=rendered_views,
                model_func=func,
            )
        return func

    return decorator


def load_model_definitions():
    """Import python modules under the models directory so the model registry is populated."""
    global _LOADED

    if _LOADED:
        return

    # Python modules under the "models" directory are imported to populate the model registry.
    MODELS_PATH = os.path.join(os.path.dirname(__file__), "models")

    count = 0
    for fname in os.listdir(MODELS_PATH):
        name = os.path.join(MODELS_PATH, fname)
        if os.path.isfile(name) and fname not in ("__init__.py", "registry.py"):
            importlib.import_module(
                f"op_analytics.datapipeline.etl.intermediate.models.{fname.removesuffix(".py")}"
            )
            count += 1

    log.info(f"Loaded {count} python modules with intermediate model definitions.")
    _LOADED = True
