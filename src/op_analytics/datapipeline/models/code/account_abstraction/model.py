from textwrap import dedent

from op_analytics.coreutils.duckdb_inmem.client import DuckDBContext, ParquetData
from op_analytics.datapipeline.models.code.account_abstraction.abis import (
    INNER_HANDLE_OP_FUNCTION_METHOD_ID_v0_6_0,
    INNER_HANDLE_OP_FUNCTION_METHOD_ID_v0_7_0,
)
from op_analytics.datapipeline.models.code.account_abstraction.event_decoders import (
    register_4337_event_decoders,
)

from op_analytics.datapipeline.models.code.account_abstraction.function_decoders import (
    register_4337_function_decoders,
)
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
        "account_abstraction/user_op_events",
        "account_abstraction/user_op_prefiltered_traces",
        "account_abstraction/user_op_sender_subtraces",
        "account_abstraction/data_quality_check_01",
        "account_abstraction/data_quality_check_02",
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
    register_4337_event_decoders(ctx)
    register_4337_function_decoders(ctx)

    # Decoded UserOperationEvent logs.
    user_ops_events = auxiliary_views["account_abstraction/user_op_events"].create_table(
        duckdb_context=ctx,
        template_parameters={
            "raw_logs": input_datasets["ingestion/logs_v1"].as_subquery(),
        },
    )

    # Table with UserOp transaction hashes. Used to filter the raw traces.
    ctx.client.sql(f"""
    CREATE OR REPLACE TABLE user_op_txhash_senders AS
    SELECT DISTINCT txhash_sender FROM {user_ops_events}
    ORDER BY transaction_hash
    """)

    # Prefiltered traces.
    user_op_prefiltered_traces = auxiliary_views[
        "account_abstraction/user_op_prefiltered_traces"
    ].create_table(
        duckdb_context=ctx,
        template_parameters={
            "raw_traces": input_datasets["ingestion/traces_v1"].as_subquery(),
            "user_op_txhash_senders": "user_op_txhash_senders",
            "inner_handle_op_method_ids": ", ".join(
                [
                    f"'{INNER_HANDLE_OP_FUNCTION_METHOD_ID_v0_6_0}'",
                    f"'{INNER_HANDLE_OP_FUNCTION_METHOD_ID_v0_7_0}'",
                ]
            ),
        },
    )

    # Traces initiated on behalf of the UserOperationEvent sender
    user_op_traces = auxiliary_views["account_abstraction/user_op_sender_subtraces"].create_table(
        duckdb_context=ctx,
        template_parameters={
            "prefiltered_traces": user_op_prefiltered_traces,
            "inner_handle_op_method_ids": ", ".join(
                [
                    f"'{INNER_HANDLE_OP_FUNCTION_METHOD_ID_v0_6_0}'",
                    f"'{INNER_HANDLE_OP_FUNCTION_METHOD_ID_v0_7_0}'",
                ]
            ),
        },
    )

    # Run data quality checks()
    data_quality_checks(ctx, auxiliary_views)

    return {}


def data_quality_checks(ctx, auxiliary_views):
    # Data Quality Validation.

    check1_msg = dedent("""\
        The total # of UserOperationEvent logs in a transaction must be <= the number 
        of sender subtraces found in the transaction.""")

    check2_msg = dedent("""\
        We should have exactly the same UserOperationEvent logs as we do first sender
        subtrace calls.""")

    check1 = (
        auxiliary_views["account_abstraction/data_quality_check_01"]
        .to_relation(duckdb_context=ctx, template_parameters={})
        .pl()
    )

    check2 = (
        auxiliary_views["account_abstraction/data_quality_check_02"]
        .to_relation(duckdb_context=ctx, template_parameters={})
        .pl()
    )

    errors = []
    if len(check1) > 0:
        errors.append(check1_msg)

    if len(check2) > 0:
        errors.append(check2_msg)

    if errors:
        raise Exception("\n\n".join(errors))
    else:
        print("DQ OK")
