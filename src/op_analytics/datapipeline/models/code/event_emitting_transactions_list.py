import duckdb

from op_analytics.datapipeline.models.compute.querybuilder import TemplatedSQLQuery
from op_analytics.datapipeline.models.compute.registry import register_model
from op_analytics.datapipeline.models.compute.types import NamedRelations


@register_model(
    input_datasets=["ingestion/logs_v1", "ingestion/transactions_v1"],
    expected_outputs=["event_emitting_transactions_list_v1"],
    auxiliary_views=[
        TemplatedSQLQuery(
            template_name="event_emitting_transactions_list",
            context={},
        ),
    ],
)
def event_emitting_transactions_list(duckdb_client: duckdb.DuckDBPyConnection) -> NamedRelations:
    return {
        "event_emitting_transactions_list_v1": duckdb_client.view(
            "event_emitting_transactions_list"
        ),
    }
