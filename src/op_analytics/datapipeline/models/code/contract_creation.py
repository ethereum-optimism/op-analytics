from op_analytics.coreutils.duckdb_inmem.client import DuckDBContext
from op_analytics.datapipeline.models.compute.model import AuxiliaryTemplate, ParquetData
from op_analytics.datapipeline.models.compute.registry import register_model
from op_analytics.datapipeline.models.compute.types import NamedRelations


@register_model(
    input_datasets=[
        "ingestion/traces_v1",
        "ingestion/transactions_v1",
    ],
    expected_outputs=[
        "create_traces_v1",
    ],
    auxiliary_templates=[
        "contract_creation_traces",
    ],
)
def contract_creation(
    ctx: DuckDBContext,
    input_datasets: dict[str, ParquetData],
    auxiliary_templates: dict[str, AuxiliaryTemplate],
) -> NamedRelations:
    traces_view = input_datasets["ingestion/traces_v1"].create_view()
    txs_view = input_datasets["ingestion/transactions_v1"].create_view()

    result = auxiliary_templates["contract_creation_traces"].to_relation(
        ctx,
        template_parameters={
            "raw_traces": traces_view,
            "raw_transactions": txs_view,
        },
    )

    return {
        "create_traces_v1": result,
    }
