import duckdb

from op_analytics.datapipeline.etl.intermediate.querybuilder import TemplatedSQLQuery
from op_analytics.datapipeline.etl.intermediate.registry import register_model
from op_analytics.datapipeline.etl.intermediate.types import NamedRelations


@register_model(
    input_datasets=["blocks", "transactions"],
    expected_outputs=["summary_v1"],
    duckdb_views=[
        TemplatedSQLQuery(
            template_name="transaction_fees",
            context={},
        ),
        TemplatedSQLQuery(
            template_name="daily_address_summary",
            context={},
        ),
    ],
)
def daily_address_summary(duckdb_client: duckdb.DuckDBPyConnection) -> NamedRelations:
    return {
        "summary_v1": duckdb_client.view("daily_address_summary"),
    }
