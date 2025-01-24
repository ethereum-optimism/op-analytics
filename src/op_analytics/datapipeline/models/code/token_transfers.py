from op_analytics.coreutils.duckdb_inmem.client import DuckDBContext, ParquetData
from op_analytics.datapipeline.models.compute.model import AuxiliaryTemplate
from op_analytics.datapipeline.models.compute.registry import register_model
from op_analytics.datapipeline.models.compute.types import NamedRelations


@register_model(
    input_datasets=[
        "ingestion/logs_v1",
    ],
    auxiliary_templates=[
        "token_transfers",
    ],
    expected_outputs=[
        "erc20_transfers_v1",
        "erc721_transfers_v1",
    ],
)
def token_transfers(
    ctx: DuckDBContext,
    input_datasets: dict[str, ParquetData],
    auxiliary_templates: dict[str, AuxiliaryTemplate],
) -> NamedRelations:
    all_transfers = auxiliary_templates["token_transfers"].to_relation(
        duckdb_context=ctx,
        template_parameters={
            "raw_logs": input_datasets["ingestion/logs_v1"].as_subquery(),
        },
    )

    erc20_transfers = all_transfers.filter("token_id IS NULL").project("* EXCLUDE token_id")
    erc721_transfers = all_transfers.filter("token_id IS NOT NULL").project(
        "* EXCLUDE (amount, amount_lossless)"
    )
    return {
        "erc20_transfers_v1": erc20_transfers,
        "erc721_transfers_v1": erc721_transfers,
    }
