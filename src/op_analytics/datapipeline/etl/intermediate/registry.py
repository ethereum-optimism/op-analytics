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
    def __call__(self, duckdb_client: duckdb.DuckDBPyConnection) -> NamedRelations: ...


class ModelInputDataReader(Protocol):
    def duckdb_relation(self, dataset) -> duckdb.DuckDBPyRelation: ...


@dataclass
class PythonModel:
    name: str
    input_datasets: list[str]
    expected_output_datasets: list[str]
    duckdb_views: list[RenderedSQLQuery]
    model_func: ModelFunction
    block_filter_pct: int

    @property
    def func(self) -> ModelFunction:
        return self.model_func


@dataclass
class PythonModelExecutor:
    model: PythonModel
    client: duckdb.DuckDBPyConnection
    data_reader: ModelInputDataReader

    def __enter__(self):
        # Register input data as views on the duckdb client.
        for dataset in self.model.input_datasets:
            if self.model.block_filter_pct is not None:
                if dataset == "blocks":
                    blockfilter = f"number % 100 <= {self.model.block_filter_pct}"
                elif dataset in ("logs", "transactions", "traces"):
                    blockfilter = f"block_number % 100 <= {self.model.block_filter_pct}"
                else:
                    blockfilter = None
            else:
                blockfilter = None

            relation = self.data_reader.duckdb_relation(dataset)

            if blockfilter is not None:
                relation = relation.filter(blockfilter)

            self.client.register(
                view_name=dataset,
                python_object=relation,
            )

        # Register the rendered views.
        for view in self.model.duckdb_views:
            self.client.register(
                view_name=view.template_name,
                python_object=self.client.sql(view.query),
            )

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Unregister all views so client is good to run other models.
        for dataset in self.model.input_datasets:
            self.client.unregister(view_name=dataset)

        for view in self.model.duckdb_views:
            self.client.unregister(view_name=view.template_name)

    def execute(self):
        return self.model.func(self.client)


REGISTERED_INTERMEDIATE_MODELS: dict[str, PythonModel] = {}


def register_model(
    input_datasets: list[str],
    expected_outputs: list[str],
    duckdb_views: list[TemplatedSQLQuery],
    block_filter_pct: int | None = None,
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
                block_filter_pct=block_filter_pct,
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
