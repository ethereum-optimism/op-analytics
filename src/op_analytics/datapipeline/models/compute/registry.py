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
    auxiliary_views: list[TemplatedSQLQuery],
):
    def decorator(func):
        model_name = func.__name__
        with bound_contextvars(model=model_name):
            REGISTERED_INTERMEDIATE_MODELS[model_name] = PythonModel(
                name=model_name,
                input_datasets=input_datasets,
                expected_output_datasets=expected_outputs,
                auxiliary_views=auxiliary_views,
                model_func=func,
            )
        return func

    return decorator


def load_model_definitions(module_names: list[str] | None = None, force=False):
    """Import python modules under the models directory so the model registry is populated.

    If module_names is provided only the requested python modules are loaded.
    """
    global _LOADED

    if _LOADED and not force:
        return

    # Python modules under the "code" directory are imported to populate the model registry.
    MODELS_PATH = os.path.join(os.path.dirname(__file__), "../code")

    count = 0
    for fname in os.listdir(MODELS_PATH):
        name = os.path.join(MODELS_PATH, fname)
        if os.path.isfile(name) and fname not in ("__init__.py"):
            module_name = fname.removesuffix(".py")

            if module_names is not None and module_name not in module_names:
                continue

            importlib.import_module(
                f"op_analytics.datapipeline.models.code.{fname.removesuffix(".py")}"
            )
            count += 1

    log.info(f"Loaded {count} python modules with intermediate model definitions.")
    _LOADED = True


def vefify_models(models: list[str]):
    for model in models:
        should_exit = False
        if model not in REGISTERED_INTERMEDIATE_MODELS:
            should_exit = True
            log.error(f"Model is not registered: {model}")
        if should_exit:
            log.error("Cannot run on unregistered models. Will exit.")
            raise Exception("unregistered intermediate model")
