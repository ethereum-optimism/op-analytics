from op_analytics.coreutils.duckdb_inmem.client import DuckDBContext
from op_analytics.datapipeline.models.compute.model import AuxiliaryTemplate, ParquetData
from op_analytics.datapipeline.models.compute.registry import register_model
from op_analytics.datapipeline.models.compute.types import NamedRelations


@register_model(
    input_datasets=[
        "ingestion/blocks_v1",
        "ingestion/transactions_v1",
    ],
    expected_outputs=[
        "summary_v2",
    ],
    auxiliary_templates=[
        "refined_transactions_fees",
        "daily_address_summary",
    ],
)
def daily_address_summary(
    ctx: DuckDBContext,
    input_datasets: dict[str, ParquetData],
    auxiliary_templates: dict[str, AuxiliaryTemplate],
) -> NamedRelations:
    blocks_view = input_datasets["ingestion/blocks_v1"].create_view()
    txs_view = input_datasets["ingestion/transactions_v1"].create_view()

    refined_txs = auxiliary_templates["refined_transactions_fees"].create_view(
        duckdb_context=ctx,
        template_parameters={
            "raw_blocks": blocks_view,
            "raw_transactions": txs_view,
            "extra_cols": [],
        },
    )

    result = auxiliary_templates["daily_address_summary"].to_relation(
        ctx,
        template_parameters={
            "refined_transactions_fees": refined_txs,
        },
    )

    return {"summary_v2": result}
