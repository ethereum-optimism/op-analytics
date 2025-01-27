import importlib
from dataclasses import dataclass

import polars as pl

from op_analytics.coreutils.duckdb_inmem.client import DuckDBContext

# All model side inputs should be defined in modules under this prefix:
PACKAGE_PREFIX = "op_analytics.datapipeline.models.sideinputs"


@dataclass
class SideInput:
    side_input_name: str

    @property
    def name(self):
        return self.side_input_name

    @property
    def sanitized_name(self):
        return self.side_input_name.replace("/", "__")

    @property
    def module_path(self):
        return self.side_input_name.replace("/", ".")

    def __post_init__(self):
        """Fetch the side input data.

        TODO: Cosider caching this operation to save time.
        """
        pass

    def to_polars(self, ctx: DuckDBContext) -> pl.DataFrame:
        # Load the side input module based on the name.
        load_path = f"{PACKAGE_PREFIX}.{self.module_path}"
        module = importlib.import_module(load_path)

        # Call the load function on the module.
        return module.load(ctx)

    def create_table(self, duckdb_context: DuckDBContext) -> str:
        """Load side input data frame and store as DuckDB table."""

        df = self.to_polars()  # noqa: F841
        statement = f"CREATE OR REPLACE TABLE {self.sanitized_name} AS SELECT * FROM df"
        try:
            duckdb_context.client.sql(statement)
        except Exception as ex:
            raise Exception(f"sql error: {self.name!r}\n{str(ex)}\n\n{statement} ") from ex

        duckdb_context.report_size()
        return self.sanitized_name
