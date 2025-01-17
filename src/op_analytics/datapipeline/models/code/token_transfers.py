import duckdb
from duckdb.typing import VARCHAR, BIGINT  # Import DuckDB's type annotations

from op_analytics.datapipeline.models.compute.querybuilder import TemplatedSQLQuery
from op_analytics.datapipeline.models.compute.registry import register_model
from op_analytics.datapipeline.models.compute.types import NamedRelations

# We can add ERC20/721/115/etc here

@register_model(
    input_datasets=["ingestion/traces_v1", "ingestion/transactions_v1"],
    expected_outputs=["native_transfers_v1"],
    auxiliary_views=[
        TemplatedSQLQuery(template_name="native_transfers", context={}),
    ],
)
def token_transfers(duckdb_client: duckdb.DuckDBPyConnection) -> NamedRelations:

    return {
        "native_transfers_v1": duckdb_client.view("native_transfers"),
    }
