from op_analytics.coreutils.duckdb_inmem.client import DuckDBContext, ParquetData
from op_analytics.datapipeline.models.compute.model import AuxiliaryTemplate
from op_analytics.datapipeline.models.compute.registry import register_model
from op_analytics.datapipeline.models.compute.types import NamedRelations


@register_model(
    input_datasets=[
        "blockbatch/contract_creation/create_traces_v1",
    ],
    auxiliary_templates=[
        "erc20_contracts/erc7802",
        "erc20_contracts/erc20_contract_creation",
    ],
    expected_outputs=[
        "erc20_contracts/erc20_contract_creation",
    ],
)
def erc20_contracts(
    ctx: DuckDBContext,
    input_datasets: dict[str, ParquetData],
    auxiliary_templates: dict[str, AuxiliaryTemplate],
) -> NamedRelations:
    erc7802 = auxiliary_templates["erc20_contracts/erc7802"].create_table(
        duckdb_context=ctx,
        template_parameters={
            "contract_creation_traces": input_datasets[
                "blockbatch/contract_creation/create_traces_v1"
            ].as_subquery(),
        },
    )

    erc20_contract_creation = auxiliary_templates[
        "erc20_contracts/erc20_contract_creation"
    ].to_relation(
        duckdb_context=ctx,
        template_parameters={
            "contract_creation_traces": input_datasets[
                "blockbatch/contract_creation/create_traces_v1"
            ].as_subquery(),
            "erc7802": erc7802,
        },
    )

    ctx.client.sql(f"DROP TABLE {erc7802}")

    return {
        "erc20_contract_creation_v1": erc20_contract_creation,
    }
