import importlib
import os
from typing import Callable

from op_coreutils.logger import structlog

from .types import NamedRelations

log = structlog.get_logger()

_LOADED = False

REGISTERED_INTERMEDIATE_MODELS: dict[str, Callable[[NamedRelations], NamedRelations]] = {}


def register_model(func):
    REGISTERED_INTERMEDIATE_MODELS[func.__name__] = func
    return func


def load_model_definitions():
    """Import python modules under the models directory so the model registry is populated."""
    global _LOADED

    if _LOADED:
        return

    # Python modules under the "models" directory are imported to populate the model registry.
    MODELS_PATH = os.path.join(os.path.dirname(__file__), "models")

    count = 0
    for basename in os.listdir(MODELS_PATH):
        name = os.path.join(MODELS_PATH, basename)
        if os.path.isfile(name) and basename not in ("__init__.py", "registry.py"):
            importlib.import_module(
                f"op_datasets.etl.intermediate.models.{basename.removesuffix(".py")}"
            )
            count += 1

    log.info(f"Loaded {count} python modules with intermediate model definitions.")
    _LOADED = True
