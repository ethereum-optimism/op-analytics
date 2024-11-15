import importlib
import os
from dataclasses import dataclass
from typing import Protocol

import duckdb
from op_analytics.coreutils.logger import structlog, bound_contextvars

from .querybuilder import TemplatedSQLQuery, RenderedSQLQuery
from .types import NamedRelations

log = structlog.get_logger()

_LOADED = False


class ModelFunction(Protocol):
    def __call__(
        self,
        duckdb_client: duckdb.DuckDBPyConnection,
        rendered_queries: dict[str, RenderedSQLQuery] | None,
    ) -> NamedRelations: ...


class ModelInputDataReader(Protocol):
    def duckdb_relation(self, dataset) -> duckdb.DuckDBPyRelation: ...


@dataclass
class PythonModel:
    name: str
    input_datasets: list[str]
    expected_output_datasets: list[str]
    rendered_queries: dict[str, RenderedSQLQuery]
    model_func: ModelFunction

    @property
    def func(self) -> ModelFunction:
        return self.model_func


@dataclass
class PythonModelExecutor:
    model: PythonModel
    client: duckdb.DuckDBPyConnection
    data_reader: ModelInputDataReader

    def input_view_name(self, dataset: str) -> str:
        return dataset

    def __enter__(self):
        # Register input data as views on the duckdb client.
        for dataset in self.model.input_datasets:
            self.client.register(
                view_name=self.input_view_name(dataset),
                python_object=self.data_reader.duckdb_relation(dataset),
            )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Unregister all views so client is good to run other models.
        for dataset in self.model.input_datasets:
            self.client.unregister(view_name=self.input_view_name(dataset))

    def execute(self):
        return self.model.func(self.client, self.model.rendered_queries)


REGISTERED_INTERMEDIATE_MODELS: dict[str, PythonModel] = {}


def register_model(
    input_datasets: list[str],
    expected_outputs: list[str],
    query_templates: list[TemplatedSQLQuery] | None = None,
):
    def decorator(func):
        model_name = func.__name__
        with bound_contextvars(model=model_name):
            rendered_queries = {}
            for q in query_templates or []:
                rendered = q.render()
                rendered_queries[rendered.name] = rendered

            REGISTERED_INTERMEDIATE_MODELS[model_name] = PythonModel(
                name=model_name,
                input_datasets=input_datasets,
                expected_output_datasets=expected_outputs,
                rendered_queries=rendered_queries,
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
