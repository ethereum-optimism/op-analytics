import duckdb

from op_analytics.datapipeline.models.compute.querybuilder import TemplatedSQLQuery
from op_analytics.datapipeline.models.compute.registry import register_model
from op_analytics.datapipeline.models.compute.types import NamedRelations


@register_model(
    input_datasets=["intermediate/enriched_transactions_v1"],
    expected_outputs=["daily_transactions_by_to_v1"],
    # TODO: Uncomment if we do this as a view (or some element as a view)
    # auxiliary_views=[
    #     TemplatedSQLQuery(
    #         template_name="daily_transactions_by_to",
    #         context={},
    #     ),
    # ],
)
def daily_transactions_by_to(duckdb_client: duckdb.DuckDBPyConnection) -> NamedRelations:
    return {
        "daily_transactions_by_to_v1": duckdb_client.view(
            """
            TODO: AGGREGATION CODE
            """
        ),
    }
