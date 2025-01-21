from op_analytics.coreutils.duckdb_inmem.client import DuckDBContext, ParquetData
from op_analytics.datapipeline.models.compute.model import AuxiliaryView
from op_analytics.datapipeline.models.compute.registry import register_model
from op_analytics.datapipeline.models.compute.types import NamedRelations

from op_analytics.datapipeline.chains.goldsky_chains import goldsky_mainnet_chains_df
from op_analytics.datapipeline.chains.across_bridge import load_across_bridge_addresses


@register_model(
    input_datasets=[
        "ingestion/logs_v1",
        "ingestion/transactions_v1",
    ],
    expected_outputs=[
        "bridging_transactions_v1",
    ],
    auxiliary_views=[
        "telepotr/bridging_transactions",
    ],
)
def telepotr(
    ctx: DuckDBContext,
    input_datasets: dict[str, ParquetData],
    auxiliary_views: dict[str, AuxiliaryView],
) -> NamedRelations:
    # TODO: Replace explicit SideInput data with something that can be passed in automatically
    #       via "input_datasets".

    # Fetch external dataframes and store them as tables in DuckDB.
    goldsky_df = goldsky_mainnet_chains_df()
    across_bridge_addresses_df = load_across_bridge_addresses(chains_df=goldsky_df)  # noqa

    ctx.client.sql("""
        CREATE TABLE IF NOT EXISTS across_bridge_metadata AS 
        SELECT * FROM across_bridge_addresses_df
    """)
    ctx.client.sql("""
        CREATE TABLE IF NOT EXISTS op_stack_chain_metadata AS
        SELECT * FROM goldsky_df
    """)

    bridging_txs = auxiliary_views["telepotr/bridging_transactions"].create_view(
        duckdb_context=ctx,
        template_parameters={
            "raw_logs": input_datasets["ingestion/logs_v1"].as_subquery(),
            "raw_transactions": input_datasets["ingestion/transactions_v1"].as_subquery(),
            "across_bridge_metadata": "across_bridge_metadata",
            "op_stack_chain_metadata": "op_stack_chain_metadata",
        },
    )

    return {
        "bridging_transactions_v1": bridging_txs,
    }
