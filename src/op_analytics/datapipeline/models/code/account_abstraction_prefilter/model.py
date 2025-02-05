from op_analytics.coreutils.duckdb_inmem.client import DuckDBContext, ParquetData

from op_analytics.datapipeline.models.compute.model import AuxiliaryTemplate
from op_analytics.datapipeline.models.compute.registry import register_model
from op_analytics.datapipeline.models.compute.types import NamedRelations


@register_model(
    input_datasets=[
        "ingestion/logs_v1",
        "ingestion/traces_v1",
    ],
    auxiliary_templates=[
        "account_abstraction_prefilter/entrypoint_logs",
        "account_abstraction_prefilter/entrypoint_prefiltered_traces",
    ],
    expected_outputs=[
        "entrypoint_logs_v1",
        "entrypoint_traces_v1",
    ],
)
def account_abstraction_prefilter(
    ctx: DuckDBContext,
    input_datasets: dict[str, ParquetData],
    auxiliary_templates: dict[str, AuxiliaryTemplate],
) -> NamedRelations:
    # EntryPoint logs.
    entrypoint_logs = auxiliary_templates[
        "account_abstraction_prefilter/entrypoint_logs"
    ].create_table(
        duckdb_context=ctx,
        template_parameters={
            "raw_logs": input_datasets["ingestion/logs_v1"].as_subquery(),
        },
    )

    # Table with EntryPoint transaction hashes. Used to filter the raw traces.
    ctx.client.sql(f"""
    CREATE OR REPLACE TABLE txhashes AS
    SELECT DISTINCT transaction_hash FROM {entrypoint_logs}
    ORDER BY transaction_hash
    """)

    # Prefiltered traces.
    entrypoint_traces = auxiliary_templates[
        "account_abstraction_prefilter/entrypoint_prefiltered_traces"
    ].create_table(
        duckdb_context=ctx,
        template_parameters={
            "raw_traces": input_datasets["ingestion/traces_v1"].as_subquery(),
            "entrypoint_txhashes": "txhashes",
        },
    )

    return {
        "entrypoint_logs_v1": entrypoint_logs,
        "entrypoint_traces_v1": entrypoint_traces,
    }
