from op_analytics.coreutils.duckdb_inmem.client import DuckDBContext, ParquetData
from op_analytics.datapipeline.models.compute.model import AuxiliaryView
from op_analytics.datapipeline.models.compute.registry import register_model
from op_analytics.datapipeline.models.compute.types import NamedRelations

from op_analytics.datapipeline.chains.goldsky_chains import goldsky_mainnet_chains_df
from op_analytics.datapipeline.chains.across_bridge import load_across_bridge_addresses

goldsky_df = goldsky_mainnet_chains_df()
across_bridge_addresses = load_across_bridge_addresses(chains_df=goldsky_df)


@register_model(
    input_datasets=[
        "ingestion/logs_v1",
        "ingestion/transactions_v1",
        "across_bridge_addresses",
    ],
    expected_outputs=[
        "telepotr_bridging_transactions_v1",
    ],
    auxiliary_views=[
        "telepotr_bridging_transactions",
    ],
)
def telepotr_bridging_transactions(
    ctx: DuckDBContext,
    input_datasets: dict[str, ParquetData],
    auxiliary_views: dict[str, AuxiliaryView],
) -> NamedRelations:
    result = auxiliary_views["telepotr_bridging_transactions"].create_table(
        duckdb_context=ctx,
        template_parameters={
            "raw_logs": input_datasets["ingestion/logs_v1"].as_subquery(),
            "raw_transactions": input_datasets["ingestion/transactions_v1"].as_subquery(),
            "across_bridge_metadata": input_datasets["across_bridge_addresses"].as_subquery(),
            "op_stack_chain_metadata": input_datasets["goldsky_df"].as_subquery(),
        },
    )

    return {
        "telepotr_bridging_transactions_v1": result,
    }
