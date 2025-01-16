from op_analytics.coreutils.duckdb_inmem.client import DuckDBContext, ParquetData
from op_analytics.datapipeline.models.compute.model import AuxiliaryView
from op_analytics.datapipeline.models.compute.registry import register_model
from op_analytics.datapipeline.models.compute.types import NamedRelations


@register_model(
    input_datasets=[
        "ingestion/logs_v1",
    ],
    expected_outputs=[
        "erc20_transfers_v1",
        "erc721_transfers_v1",
    ],
    auxiliary_views=[
        "token_transfers",
    ],
)
def token_transfers(
    ctx: DuckDBContext,
    input_datasets: dict[str, ParquetData],
    auxiliary_views: dict[str, AuxiliaryView],
) -> NamedRelations:
    logs_view = input_datasets["ingestion/logs_v1"].create_view()

    all_transfers = auxiliary_views["token_transfers"].to_relation(
        duckdb_context=ctx,
        template_parameters={
            "raw_logs": logs_view,
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
