from dataclasses import dataclass, field
from typing import Protocol

import duckdb

from .model import PythonModel


class ModelInputDataReader(Protocol):
    def register_duckdb_relation(
        self,
        dataset: str,
        first_n_parquet_files: int | None = None,
    ) -> str: ...


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
                raise Exception(
                    f"sql error on rendered view: {view.template_name!r}\n{str(ex)} "
                ) from ex

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
