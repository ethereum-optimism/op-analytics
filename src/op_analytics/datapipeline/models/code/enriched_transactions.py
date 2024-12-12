import duckdb

from op_analytics.datapipeline.models.compute.querybuilder import TemplatedSQLQuery
from op_analytics.datapipeline.models.compute.registry import register_model
from op_analytics.datapipeline.models.compute.types import NamedRelations


@register_model(
    input_datasets=["ingestion/transactions_v1", "ingestion/blocks_v1"],
    expected_outputs=["enriched_transactions_v1"],
    auxiliary_views=[
        TemplatedSQLQuery(
            template_name="enriched_transactions",
            context={},
        ),
    ],
)
def enriched_transactions(duckdb_client: duckdb.DuckDBPyConnection) -> NamedRelations:
    return {
        "enriched_transactions_v1": duckdb_client.view("enriched_transactions.sql"),
    }
