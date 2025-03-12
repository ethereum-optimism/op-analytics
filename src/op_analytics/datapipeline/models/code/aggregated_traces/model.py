from op_analytics.coreutils.duckdb_inmem.client import DuckDBContext, ParquetData
from op_analytics.coreutils.logger import structlog
from op_analytics.datapipeline.models.compute.model import AuxiliaryTemplate
from op_analytics.datapipeline.models.compute.registry import register_model
from op_analytics.datapipeline.models.compute.types import NamedRelations

log = structlog.get_logger()


@register_model(
    input_datasets=[
        "blockbatch/refined_traces/refined_traces_fees_v1",
        "blockbatch/refined_traces/refined_transactions_fees_v1",
    ],
    auxiliary_templates=[
        "aggregated_traces/tr_from_tr_to_hash",
        "aggregated_traces/tr_to_hash",
    ],
    expected_outputs=[
        "agg_traces_tr_from_tr_to_hash_v1",
        "agg_traces_tr_to_hash_v1",
    ],
)
def aggregated_traces(
    ctx: DuckDBContext,
    input_datasets: dict[str, ParquetData],
    auxiliary_templates: dict[str, AuxiliaryTemplate],
) -> NamedRelations:
    agg1 = auxiliary_templates["aggregated_traces/tr_from_tr_to_hash"].create_table(
        duckdb_context=ctx,
        template_parameters={
            "refined_traces_fees": input_datasets[
                "blockbatch/refined_traces/refined_traces_fees_v1"
            ].as_subquery(),
            "refined_txs_fees": input_datasets[
                "blockbatch/refined_traces/refined_transactions_fees_v1"
            ].as_subquery(),
        },
    )

    agg2 = auxiliary_templates["aggregated_traces/tr_to_hash"].create_table(
        duckdb_context=ctx, template_parameters={"tr_from_tr_to_hash": agg1}
    )

    return {
        "agg_traces_tr_from_tr_to_hash_v1": agg1,
        "agg_traces_tr_to_hash_v1": agg2,
    }
