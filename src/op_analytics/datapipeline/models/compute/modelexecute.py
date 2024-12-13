import importlib
from dataclasses import dataclass, field
from typing import ClassVar, Protocol

import duckdb

from .querybuilder import TemplatedSQLQuery, RenderedSQLQuery
from .types import NamedRelations


class ModelFunction(Protocol):
    def __call__(self, duckdb_client: duckdb.DuckDBPyConnection) -> NamedRelations: ...


class ModelInputDataReader(Protocol):
    def register_duckdb_relation(
        self,
        dataset: str,
        first_n_parquet_files: int | None = None,
    ) -> str: ...


@dataclass
class PythonModel:
    # The registry stores all instances of PythonModel
    _registry: ClassVar[dict[str, "PythonModel"]] = {}

    # Rendered views caches rendered views so we don't have
    # to load and render templates each time.
    _rendered_views: list[RenderedSQLQuery] | None = field(init=False, default=None)

    name: str
    input_datasets: list[str]
    expected_output_datasets: list[str]
    auxiliary_views: list[TemplatedSQLQuery]
    model_func: ModelFunction

    def __post_init__(self):
        self._registry[self.name] = self

    @classmethod
    def get(cls, model_name: str) -> "PythonModel":
        if model_name not in cls._registry:
            # Import the module so that the model gets registered.
            # For now the module and model name are the same by convention, but
            # that can change if we need to.
            importlib.import_module(f"op_analytics.datapipeline.models.code.{model_name}")

        return cls._registry[model_name]

    @property
    def func(self) -> ModelFunction:
        return self.model_func

    def rendered_views(self) -> list[RenderedSQLQuery]:
        if self._rendered_views is None:
            result = []
            for q in self.auxiliary_views or []:
                rendered = q.render()
                result.append(rendered)
            self._rendered_views = result

        return self._rendered_views


@dataclass
class PythonModelExecutor:
    model: PythonModel
    client: duckdb.DuckDBPyConnection
    data_reader: ModelInputDataReader
    limit_input_parquet_files: int | None = None

    # Keep track of registered views so they can be unregistered
    # at exit.
    registered_views: list[str] = field(default_factory=list, init=False)

    def __enter__(self):
        # Register input data as views on the duckdb client.
        for dataset in self.model.input_datasets:
            view_name = self.data_reader.register_duckdb_relation(
                dataset=dataset,
                first_n_parquet_files=self.limit_input_parquet_files,
            )
            self.registered_views.append(view_name)

        # Register the rendered views.
        for view in self.model.rendered_views():
            try:
                obj = self.client.sql(view.query)
            except Exception as ex:
                raise Exception(f"sql error on rendered view: {view!r}") from ex

            self.client.register(
                view_name=view.template_name,
                python_object=obj,
            )
            self.registered_views.append(view.template_name)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Unregister all views so client is good to run other models.
        for view in self.registered_views:
            self.client.unregister(view_name=view)

        # Ensure there are no user views remaining.
        remaining_views = (
            self.client.sql("SELECT view_name FROM duckdb_views() WHERE NOT internal")
            .pl()["view_name"]
            .to_list()
        )
        for view in remaining_views:
            self.client.unregister(view_name=view)

    def execute(self):
        return self.model.func(self.client)
