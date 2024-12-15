import duckdb

from op_analytics.datapipeline.models.compute.querybuilder import TemplatedSQLQuery
from op_analytics.datapipeline.models.compute.registry import register_model
from op_analytics.datapipeline.models.compute.types import NamedRelations


@register_model(
    input_datasets=[
        "ingestion/transactions_v1",
        "ingestion/blocks_v1",
        "ingestion/traces_v1",
    ],
    expected_outputs=[
        "refined_trace_calls_v1",
        "refined_trace_calls_agg_tr_from_tr_to_hash_v1",
        "daily_trace_calls_agg_tr_to_tx_to_v1",
        "daily_trace_calls_agg_to_v1",
    ],
    auxiliary_views=[
        TemplatedSQLQuery(
            template_name="refined_transactions_fees",
            context={},
        ),
        TemplatedSQLQuery(
            template_name="refined_trace_calls",
            context={},
        ),
        TemplatedSQLQuery(
            template_name="refined_trace_calls_agg_tr_from_tr_to_hash",
            context={},
        ),
        TemplatedSQLQuery(
            template_name="refined_trace_calls_agg_tr_to_hash",
            context={},
        ),
        TemplatedSQLQuery(
            template_name="daily_trace_calls_agg_tr_to_tx_to",
            context={},
        ),
        TemplatedSQLQuery(
            template_name="daily_trace_calls_agg_tr_to",
            context={},
        ),
    ],
)
def refined_traces_models(
    duckdb_client: duckdb.DuckDBPyConnection,
) -> NamedRelations:
    return {
        "refined_trace_calls_v1": duckdb_client.view("refined_trace_calls"),
        "refined_trace_calls_agg_tr_from_tr_to_hash_v1": duckdb_client.view(
            "refined_trace_calls_agg_tr_from_tr_to_hash"
        ),
        "refined_trace_calls_agg_to_hash_v1": duckdb_client.view(
            "refined_trace_calls_agg_tr_to_hash"
        ),
        "daily_trace_calls_agg_tr_to_tx_to_v1": duckdb_client.view(
            "daily_trace_calls_agg_tr_to_tx_to"
        ),
        "daily_trace_calls_agg_to_v1": duckdb_client.view("daily_trace_calls_agg_tr_to"),
    }
