from dataclasses import dataclass, field

import duckdb

from op_analytics.coreutils.duckdb_inmem.client import DuckDBContext
from op_analytics.coreutils.logger import structlog

from .model import PythonModel, AuxiliaryTemplate, ModelDataReader, ParquetData


log = structlog.get_logger()


@dataclass
class PythonModelExecutor:
    model: PythonModel
    duckdb_context: DuckDBContext
    data_reader: ModelDataReader
    limit_input_parquet_files: int | None = None

    # Input datasets as remote parquet paths.
    input_datasets: dict[str, ParquetData] = field(default_factory=dict, init=False)

    # Aux views.
    auxiliary_templates: dict[str, AuxiliaryTemplate] = field(default_factory=dict, init=False)

    # Keep track of registered views so they can be unregistered at exit.
    registered_views: list[str] = field(default_factory=list, init=False)

    @property
    def client(self) -> duckdb.DuckDBPyConnection:
        return self.duckdb_context.client

    def __enter__(self):
        # Initialize remote parquet data.
        for dataset in self.model.input_datasets:
            self.input_datasets[dataset] = self.data_reader.remote_parquet(
                dataset=dataset,
                first_n_parquet_files=self.limit_input_parquet_files,
            )

        # Initialize the auxiliary views:
        for template_name in self.model.auxiliary_templates:
            self.auxiliary_templates[template_name] = AuxiliaryTemplate(template_name=template_name)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Unregister all views so client is good to run other models.
        for view in self.registered_views:
            self.client.unregister(view_name=view)

        # Ensure there are no user views remaining.
        self.duckdb_context.unregister_views()

        # Close the context to premanently delete duckdb storage from disk.
        self.duckdb_context.close(remove_db_path=True)

    def call_args(self):
        return (self.duckdb_context, self.input_datasets, self.auxiliary_templates)

    def execute(self):
        return self.model.func(*self.call_args())
