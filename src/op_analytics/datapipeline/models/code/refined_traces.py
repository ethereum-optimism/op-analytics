from op_analytics.coreutils.duckdb_inmem.client import DuckDBContext, ParquetData
from op_analytics.datapipeline.models.compute.model import AuxiliaryTemplate
from op_analytics.datapipeline.models.compute.registry import register_model
from op_analytics.datapipeline.models.compute.types import NamedRelations


@register_model(
    input_datasets=[
        "ingestion/blocks_v1",
        "ingestion/traces_v1",
        "ingestion/transactions_v1",
    ],
    auxiliary_templates=[
        "refined_transactions_fees",
        "refined_traces/traces_projection",
        "refined_traces/traces_with_gas_used",
        "refined_traces/traces_txs_join",
    ],
    expected_outputs=[
        "refined_transactions_fees_v2",
        "refined_traces_fees_v2",
    ],
)
def refined_traces(
    ctx: DuckDBContext,
    input_datasets: dict[str, ParquetData],
    auxiliary_templates: dict[str, AuxiliaryTemplate],
) -> NamedRelations:
    # Start out by adding fees and other useful fields to each transaction.
    refined_txs = auxiliary_templates["refined_transactions_fees"].create_table(
        duckdb_context=ctx,
        template_parameters={
            "raw_blocks": input_datasets["ingestion/blocks_v1"].as_subquery(),
            "raw_transactions": input_datasets["ingestion/transactions_v1"].as_subquery(),
            "extra_cols": [],
        },
    )

    # Project only the necessary fields from raw traces.
    refined_traces_projection = auxiliary_templates["refined_traces/traces_projection"].create_view(
        duckdb_context=ctx,
        template_parameters={
            "raw_traces": input_datasets["ingestion/traces_v1"].as_subquery(),
        },
    )

    # Add up the gas used by the subtraces on each trace. Also include the
    # number of traces in the parent transaction, so that the transaction gas
    # used and fees can be amortized among traces.
    traces_with_gas_used = auxiliary_templates["refined_traces/traces_with_gas_used"].create_view(
        duckdb_context=ctx,
        template_parameters={
            "refined_traces_projection": refined_traces_projection,
        },
    )

    # Joins traces with transactions. Amorizes the transaction gas used and
    # fees across traces.
    traces_txs_join = auxiliary_templates["refined_traces/traces_txs_join"].create_view(
        duckdb_context=ctx,
        template_parameters={
            "traces_with_gas_used": traces_with_gas_used,
            "refined_transactions_fees": refined_txs,
        },
    )

    return {
        "refined_transactions_fees_v2": refined_txs,
        "refined_traces_fees_v2": traces_txs_join,
    }
