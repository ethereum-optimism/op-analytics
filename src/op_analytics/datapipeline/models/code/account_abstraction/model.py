from op_analytics.coreutils.duckdb_inmem.client import DuckDBContext, ParquetData
from op_analytics.coreutils.logger import memory_usage, structlog
from op_analytics.datapipeline.models.code.account_abstraction.abis import (
    HANDLE_OPS_FUNCTION_METHOD_ID_v0_6_0,
    HANDLE_OPS_FUNCTION_METHOD_ID_v0_7_0,
    INNER_HANDLE_OP_FUNCTION_METHOD_ID_v0_6_0,
    INNER_HANDLE_OP_FUNCTION_METHOD_ID_v0_7_0,
)
from op_analytics.datapipeline.models.code.account_abstraction.decoders import (
    register_4337_decoders,
)
from op_analytics.datapipeline.models.compute.model import AuxiliaryTemplate
from op_analytics.datapipeline.models.compute.registry import register_model
from op_analytics.datapipeline.models.compute.types import NamedRelations

log = structlog.get_logger()


@register_model(
    input_datasets=[
        "blockbatch/account_abstraction_prefilter/entrypoint_logs_v1",
        "blockbatch/account_abstraction_prefilter/entrypoint_traces_v1",
    ],
    auxiliary_templates=[
        "account_abstraction/useroperationevent_logs",
        "account_abstraction/enriched_entrypoint_traces",
        "account_abstraction/data_quality_check_01",
        "account_abstraction/data_quality_check_02",
    ],
    expected_outputs=[
        "useroperationevent_logs_v2",
        "enriched_entrypoint_traces_v2",
    ],
    excluded_chains=[
        # Exclude chains for which we don't have traces data.
        "kroma",
        "unichain_sepolia",
    ],
)
def account_abstraction(
    ctx: DuckDBContext,
    input_datasets: dict[str, ParquetData],
    auxiliary_templates: dict[str, AuxiliaryTemplate],
) -> NamedRelations:
    register_4337_decoders(ctx)

    # Decoded UserOperationEvent logs.
    user_ops = auxiliary_templates["account_abstraction/useroperationevent_logs"].create_table(
        duckdb_context=ctx,
        template_parameters={
            "raw_logs": input_datasets[
                "blockbatch/account_abstraction_prefilter/entrypoint_logs_v1"
            ].as_subquery(),
        },
    )

    log.info("memory usage after user ops", max_rss=memory_usage())

    # Persist the prefiltered traces for performance gains.
    prefiltered_traces = input_datasets[
        "blockbatch/account_abstraction_prefilter/entrypoint_traces_v1"
    ].create_table()

    log.info("memory usage after prefiltered", max_rss=memory_usage())

    # Traces initiated on behalf of the UserOperationEvent sender
    entrypoint_traces = auxiliary_templates[
        "account_abstraction/enriched_entrypoint_traces"
    ].create_view(
        duckdb_context=ctx,
        template_parameters={
            "prefiltered_traces": prefiltered_traces,
            "uops": user_ops,
            "method_id_v6": INNER_HANDLE_OP_FUNCTION_METHOD_ID_v0_6_0,
            "method_id_v7": INNER_HANDLE_OP_FUNCTION_METHOD_ID_v0_7_0,
            "handle_ops_v6": HANDLE_OPS_FUNCTION_METHOD_ID_v0_6_0,
            "handle_ops_v7": HANDLE_OPS_FUNCTION_METHOD_ID_v0_7_0,
        },
    )

    log.info("memory usage after enriched traces", max_rss=memory_usage())

    # Data Quality Checks
    errors: list[tuple[str, dict]] = []
    for tpl_name, tpl in auxiliary_templates.items():
        if "data_quality_check" in tpl_name:
            template_errors = tpl.run_as_data_quality_check(duckdb_context=ctx)
            errors.extend((tpl_name, err) for err in template_errors)

    if errors:
        log.error("failed data quality")
        for tpl_name, err in errors[:10]:
            log.error(f"{tpl_name}: {err}")

        raise Exception("\n\n".join(f"{tpl_name}: {err}" for tpl_name, err in errors))
    else:
        log.info("Data Quality OK")

    log.info("memory usage after data quality", max_rss=memory_usage())

    return {
        "useroperationevent_logs_v2": user_ops,
        "enriched_entrypoint_traces_v2": entrypoint_traces,
    }
