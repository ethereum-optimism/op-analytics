import duckdb

from op_analytics.datapipeline.models.compute.querybuilder import TemplatedSQLQuery
from op_analytics.datapipeline.models.compute.registry import register_model
from op_analytics.datapipeline.models.compute.types import NamedRelations


@register_model(
    input_datasets=["ingestion/traces_v1", "extended_transactions_fees_v1"],
    expected_outputs=["enriched_trace_calls_v1"],
    auxiliary_views=[
        TemplatedSQLQuery(
            template_name="enriched_trace_calls",
            context={},
        ),
    ],
)
def enriched_trace_calls(duckdb_client: duckdb.DuckDBPyConnection) -> NamedRelations:
    return {
        "enriched_trace_calls_v1": duckdb_client.view("enriched_trace_calls"),
    }
