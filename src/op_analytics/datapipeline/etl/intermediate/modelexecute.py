from dataclasses import dataclass
from typing import Protocol

import duckdb

from .querybuilder import RenderedSQLQuery
from .types import NamedRelations


class ModelFunction(Protocol):
    def __call__(self, duckdb_client: duckdb.DuckDBPyConnection) -> NamedRelations: ...


class ModelInputDataReader(Protocol):
    def duckdb_relation(
        self,
        dataset: str,
        first_n_parquet_files: int | None = None,
    ) -> duckdb.DuckDBPyRelation: ...


@dataclass
class PythonModel:
    name: str
    input_datasets: list[str]
    expected_output_datasets: list[str]
    duckdb_views: list[RenderedSQLQuery]
    model_func: ModelFunction

    @property
    def func(self) -> ModelFunction:
        return self.model_func


def sanitized_table_name(dataset_name: str) -> str:
    return dataset_name.replace("/", "_")


@dataclass
class PythonModelExecutor:
    model: PythonModel
    client: duckdb.DuckDBPyConnection
    data_reader: ModelInputDataReader
    limit_input_parquet_files: int | None = None

    def __enter__(self):
        # Register input data as views on the duckdb client.
        for dataset in self.model.input_datasets:
            self.client.register(
                view_name=sanitized_table_name(dataset),
                python_object=self.data_reader.duckdb_relation(
                    dataset=dataset,
                    first_n_parquet_files=self.limit_input_parquet_files,
                ),
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
