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
    expected_outputs=[
        "refined_transactions_fees_v1",
        "event_emitting_transactions_v1",
        "summary_v1",
        "daily_transactions_agg_tx_from_tx_to_method_v1",
        "daily_transactions_agg_tx_to_method_v1",
        "daily_transactions_agg_tx_to_v1",
    ],
    auxiliary_views=[
        TemplatedSQLQuery(
            template_name="refined_transactions_fees",
            context={},
        ),
        TemplatedSQLQuery(
            template_name="logs_topic0_filters",
            context={},
        ),
        TemplatedSQLQuery(
            template_name="event_emitting_transactions",
            context={},
        ),
        TemplatedSQLQuery(
            template_name="daily_address_summary",
            context={},
        ),
        TemplatedSQLQuery(
            template_name="daily_transactions_agg_tx_from_tx_to_method",
            context={},
        ),
        TemplatedSQLQuery(
            template_name="daily_transactions_agg_tx_to_method",
            context={},
        ),
        TemplatedSQLQuery(
            template_name="daily_transactions_agg_tx_to",
            context={},
        ),
    ],
)
def refined_transactions_addresses_models(
    duckdb_client: duckdb.DuckDBPyConnection,
) -> NamedRelations:
    return {
        "refined_transactions_fees_v1": duckdb_client.view("refined_transactions_fees"),
        "event_emitting_transactions_v1": duckdb_client.view("event_emitting_transactions"),
        "summary_v1": duckdb_client.view("daily_address_summary"),
        "daily_transactions_agg_tx_from_tx_to_method_v1": duckdb_client.view(
            "daily_transactions_agg_tx_from_tx_to_method"
        ),
        "daily_transactions_agg_tx_to_method_v1": duckdb_client.view(
            "daily_transactions_agg_tx_to_method"
        ),
        "daily_transactions_agg_tx_to_v1": duckdb_client.view("daily_transactions_agg_tx_to"),
    }
