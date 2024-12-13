import duckdb

from op_analytics.datapipeline.models.compute.querybuilder import TemplatedSQLQuery
from op_analytics.datapipeline.models.compute.registry import register_model
from op_analytics.datapipeline.models.compute.types import NamedRelations


@register_model(
    input_datasets=["ingestion/logs_v1"],
    expected_outputs=["extended_transactions_fees.sql_v1"],
    auxiliary_views=[
        TemplatedSQLQuery(
            template_name="extended_transactions_fees",
            context={},
        ),
    ],
)
def extended_transactions_fees(duckdb_client: duckdb.DuckDBPyConnection) -> NamedRelations:
    return {
        "extended_transactions_fees_v1": duckdb_client.view("extended_transactions_fees"),
    }
