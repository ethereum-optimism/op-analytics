import duckdb

from op_analytics.datapipeline.models.compute.querybuilder import TemplatedSQLQuery
from op_analytics.datapipeline.models.compute.registry import register_model
from op_analytics.datapipeline.models.compute.types import NamedRelations


@register_model(
    input_datasets=[
        "ingestion/transactions_v1",
        "ingestion/blocks_v1",
        "ingestion/logs_v1",
    ],
    expected_outputs=["refined_transactions_fees_v1"],
    auxiliary_views=[
        TemplatedSQLQuery(
            template_name="refined_transactions_fees",
            context={},
        ),
    ],
)
def refined_transactions_fees(duckdb_client: duckdb.DuckDBPyConnection) -> NamedRelations:
    return {
        "refined_transactions_fees_v1": duckdb_client.view("refined_transactions_fees"),
    }
