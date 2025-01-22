from op_analytics.coreutils.duckdb_inmem.client import DuckDBContext, ParquetData
from op_analytics.datapipeline.models.compute.model import AuxiliaryView
from op_analytics.datapipeline.models.compute.registry import register_model
from op_analytics.datapipeline.models.compute.types import NamedRelations


@register_model(
    input_datasets=[
        "ingestion/blocks_v1",
        "ingestion/logs_v1",
        "ingestion/traces_v1",
        "ingestion/transactions_v1",
    ],
    auxiliary_views=[
        "refined_transactions_fees",
        "account_abstraction/user_ops",
        "account_abstraction/user_ops_b",
        "account_abstraction/handle_ops",
    ],
    expected_outputs=[
        "user_ops_v1",
        "handle_ops_v1",
        "user_txs_v1",
    ],
)
def account_abstraction(
    ctx: DuckDBContext,
    input_datasets: dict[str, ParquetData],
    auxiliary_views: dict[str, AuxiliaryView],
) -> NamedRelations:
    from op_analytics.datapipeline.models.code.account_abstraction.event_user_op import (
        register_decode_user_ops,
    )

    register_decode_user_ops(ctx)

    # UserOperationEvent
    user_ops_events = auxiliary_views["account_abstraction/user_ops"].create_table(
        duckdb_context=ctx,
        template_parameters={
            "raw_logs": input_datasets["ingestion/logs_v1"].as_subquery(),
        },
    )

    # Filter raw transactions to the ones having UserOperationEvent logs.
    # This is a lazy operation. Returns the raw SQL string.
    filtered_transactions = input_datasets["ingestion/transactions_v1"].select_string(
        projections="read_parquet.*",
        parenthesis=True,
        additional_sql=f"""
        INNER JOIN (SELECT DISTINCT block_number, transaction_hash FROM {user_ops_events}) ops
        ON read_parquet.block_number = ops.block_number
        AND read_parquet.hash = ops.transaction_hash
        """,
    )

    # Create a table where the filtered transactions are enhanced with the refined
    # transactions fees transformation.
    refined_txs = auxiliary_views["refined_transactions_fees"].create_table(
        duckdb_context=ctx,
        template_parameters={
            "raw_blocks": input_datasets["ingestion/blocks_v1"].as_subquery(),
            # "raw_transactions": input_datasets["ingestion/transactions_v1"].as_subquery(),
            "raw_transactions": filtered_transactions,
            "extra_cols": ["t.input"],
        },
    )
    print(refined_txs)

    # --------------

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

    return {}
