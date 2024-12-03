import duckdb

from op_analytics.datapipeline.etl.intermediate.querybuilder import TemplatedSQLQuery
from op_analytics.datapipeline.etl.intermediate.registry import register_model
from op_analytics.datapipeline.etl.intermediate.types import NamedRelations


@register_model(
    input_datasets=["ingestion/traces_v1", "ingestion/transactions_v1"],
    expected_outputs=["create_traces_v1"],
    duckdb_views=[
        TemplatedSQLQuery(
            template_name="contract_creation_traces",
            context={},
        ),
    ],
)
def contract_creation(duckdb_client: duckdb.DuckDBPyConnection) -> NamedRelations:
    return {
        "create_traces_v1": duckdb_client.view("contract_creation_traces"),
    }
