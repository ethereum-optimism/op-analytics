from op_analytics.coreutils.duckdb_inmem.client import DuckDBContext, ParquetData
from op_analytics.datapipeline.models.compute.model import AuxiliaryTemplate
from op_analytics.datapipeline.models.compute.registry import register_model
from op_analytics.datapipeline.models.compute.types import NamedRelations


@register_model(
    input_datasets=[
        "ingestion/traces_v1",
    ],
    auxiliary_templates=[
        "native_transfers",
    ],
    expected_outputs=[
        "native_transfers_v1",
    ],
)
def native_transfers(
    ctx: DuckDBContext,
    input_datasets: dict[str, ParquetData],
    auxiliary_templates: dict[str, AuxiliaryTemplate],
) -> NamedRelations:
    native_transfers = auxiliary_templates["native_transfers"].to_relation(
        duckdb_context=ctx,
        template_parameters={
            "raw_traces": input_datasets["ingestion/traces_v1"].as_subquery(),
        },
    )

    return {
        "native_transfers_v1": native_transfers,
    }
