from op_analytics.coreutils.duckdb_inmem.client import DuckDBContext, ParquetData
from op_analytics.datapipeline.models.compute.model import AuxiliaryView
from op_analytics.datapipeline.models.compute.registry import register_model
from op_analytics.datapipeline.models.compute.types import NamedRelations


@register_model(
    input_datasets=[
        "ingestion/blocks_v1",
        "ingestion/traces_v1",
        "ingestion/transactions_v1",
    ],
    auxiliary_views=[
        "refined_transactions_fees",
        "refined_traces/traces_projection",
        "refined_traces/traces_amortized",
        "refined_traces/traces_txs_join",
    ],
    expected_outputs=[
        "refined_transactions_fees_v1",
        "refined_traces_fees_v1",
    ],
)
def refined_traces(
    ctx: DuckDBContext,
    input_datasets: dict[str, ParquetData],
    auxiliary_views: dict[str, AuxiliaryView],
) -> NamedRelations:
    # Start out by adding fees and other useful fields to each transaction.
    refined_txs = auxiliary_views["refined_transactions_fees"].create_table(
        duckdb_context=ctx,
        template_parameters={
            "raw_blocks": input_datasets["ingestion/blocks_v1"].as_subquery(),
            "raw_transactions": input_datasets["ingestion/transactions_v1"].as_subquery(),
        },
    )

    # Project only the necessary fields from raw traces.
    refined_traces_projection = auxiliary_views["refined_traces/traces_projection"].create_table(
        duckdb_context=ctx,
        template_parameters={
            "raw_traces": input_datasets["ingestion/traces_v1"].as_subquery(),
        },
    )

    # Add up the gas used by the subtraces on each trace. Also include the
    # number of traces in the parent transaction, so that the transaction gas
    # used and fees can be amortized among traces.
    traces_with_gas_used = auxiliary_views["refined_traces/traces_with_gas_used"].create_table(
        duckdb_context=ctx,
        template_parameters={
            "refined_traces_projection": refined_traces_projection,
        },
    )

    # Joins traces with transactions. Amorizes the transaction gas used and
    # fees across traces.
    traces_txs_join = auxiliary_views["refined_traces/traces_txs_join"].create_table(
        duckdb_context=ctx,
        template_parameters={
            "traces_with_gas_used": traces_with_gas_used,
            "refined_transactions_fees": refined_txs,
        },
    )

    # These two tables were materialized in duckdb temporarily (with an ORDER BY)
    # to improve the performance of the traces <> txs join. We don't need them
    # anymore as the joined result is also materialized.
    ctx.client.sql(f"DROP TABLE {refined_traces_projection}")
    ctx.client.sql(f"DROP TABLE {traces_with_gas_used}")

    return {
        "refined_transactions_fees_v1": refined_txs,
        "refined_traces_fees_v1": traces_txs_join,
    }
