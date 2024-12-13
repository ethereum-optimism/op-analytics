import duckdb

from op_analytics.datapipeline.models.compute.querybuilder import TemplatedSQLQuery
from op_analytics.datapipeline.models.compute.registry import register_model
from op_analytics.datapipeline.models.compute.types import NamedRelations


@register_model(
    input_datasets=["intermediate/refined_transactions_fees_v1"],
    expected_outputs=["summary_v1"],
    auxiliary_views=[
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
