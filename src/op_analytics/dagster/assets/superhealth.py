'''
Network Lineage Dagster Assets

This file contains Dagster assets representing a hierarchical network of BigQuery views
and materialized tables. The dependencies are structured to show the actual flow:

Base Tables → Views → Materialized Tables → Dependent Views

Assets use deps=[] framework to create proper DAG relationships.
'''

from dagster import OpExecutionContext, asset
import polars as pl


@asset(metadata={
    "table_name": "github_devs",
    "group_id": "github_devs_table_1f57d3bd",
    "columns": [
        "created_at",
        "date",
        "dt",
        "id",
        "updated_at"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def github_devs_table_1f57d3bd(context: OpExecutionContext):
    """Base table column group: github_devs\nColumns: 5"""
    context.log.info("Processing base table: github_devs")
    # Base table asset implementation would go here


# =======================================================================
# Base Table Column Assets
# =======================================================================

@asset(metadata={
    "table_name": "oplabs-tools-data.dailydata_defillama.protocols_metadata_v1",
    "group_id": "protocols_metadata_v1_table_170c024c",
    "columns": [
        "dt",
        "misrepresented_tokens",
        "parent_protocol",
        "protocol_category",
        "protocol_name",
        "protocol_slug",
        "wrong_liquidity"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def protocols_metadata_v1_table_170c024c(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.dailydata_defillama.protocols_metadata_v1\nColumns: 7"""
    context.log.info("Processing base table: oplabs-tools-data.dailydata_defillama.protocols_metadata_v1")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "oplabs-tools-data.dailydata_defillama.volume_fees_revenue_breakdown_v1",
    "group_id": "volume_fees_revenue_breakdown_v1_table_4e626584",
    "columns": [
        "breakdown_name",
        "chain",
        "dt",
        "total_fees_usd",
        "total_revenue_usd",
        "total_volume_usd"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def volume_fees_revenue_breakdown_v1_table_4e626584(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.dailydata_defillama.volume_fees_revenue_breakdown_v1\nColumns: 6"""
    context.log.info("Processing base table: oplabs-tools-data.dailydata_defillama.volume_fees_revenue_breakdown_v1")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "artemis-bigquery-share.optimism.ez_github_developer_commits",
    "group_id": "ez_github_developer_commits_table_549079d0",
    "columns": [
        "AUTHOR_ID",
        "ID",
        "NUM_ADDITIONS",
        "NUM_COMMITS",
        "NUM_DELETIONS",
        "REPO_FULL_NAME",
        "START_OF_WEEK"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def ez_github_developer_commits_table_549079d0(context: OpExecutionContext):
    """Base table column group: artemis-bigquery-share.optimism.ez_github_developer_commits\nColumns: 7"""
    context.log.info("Processing base table: artemis-bigquery-share.optimism.ez_github_developer_commits")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "oplabs-tools-data.rpc_table_uploads.hourly_cumulative_l2_revenue_snapshots",
    "group_id": "hourly_cumulative_l2_revenue_snapshots_table_c63d3c58",
    "columns": [
        "alltime_revenue_native",
        "block_number",
        "block_time",
        "chain_id",
        "chain_name",
        "vault_address",
        "vault_name"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def hourly_cumulative_l2_revenue_snapshots_table_c63d3c58(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.rpc_table_uploads.hourly_cumulative_l2_revenue_snapshots\nColumns: 7"""
    context.log.info(
        "Processing base table: oplabs-tools-data.rpc_table_uploads.hourly_cumulative_l2_revenue_snapshots")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "oplabs-tools-data.materialized_tables.daily_opcollective_revshare_mv",
    "group_id": "daily_opcollective_revshare_mv_table_1488e45c",
    "columns": [
        "actual_collective_contribution",
        "actual_collective_contribution_rec",
        "alignment",
        "avg_eth_usd_price",
        "chain_governor_profit_estimated",
        "chain_governor_profit_estimated_usd",
        "chain_id",
        "chain_layer",
        "chain_name",
        "chart_name",
        "display_name",
        "dt_day",
        "gas_token",
        "is_flagged",
        "l1_costs",
        "l1_costs_as_superchain",
        "l2_revenue",
        "l2_revenue_as_superchain",
        "l2_txs",
        "l2_txs_as_superchain",
        "latest_flag",
        "net_onchain_profit",
        "net_onchain_profit_as_superchain",
        "net_onchain_profit_as_superchain_usd",
        "op_chain_start",
        "prior_flag",
        "raas_deployer",
        "recency_rank",
        "revshare_estimated",
        "revshare_estimated_usd",
        "revshare_if_profit",
        "revshare_if_revenue"
    ],
    "usage_count": 0,
    "asset_type": "materialized_table",
    "source_view"

    : "oplabs-tools-data.views.daily_opcollective_revshare"
})
def daily_opcollective_revshare_mv_table_1488e45c(context: OpExecutionContext):
    """Materialized table from view: oplabs-tools-data.views.daily_opcollective_revshare\nTable: oplabs-tools-data.materialized_tables.daily_opcollective_revshare_mv"""
    context.log.info("Processing base table: oplabs-tools-data.materialized_tables.daily_opcollective_revshare_mv")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "oplabs-tools-data.api_table_uploads.dune_all_gas",
    "group_id": "dune_all_gas_table_7416ee25",
    "columns": [
        "blockchain",
        "chain_id",
        "dt",
        "last_updated",
        "source",
        "sum_evm_gas_used"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def dune_all_gas_table_7416ee25(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.api_table_uploads.dune_all_gas\nColumns: 6"""
    context.log.info("Processing base table: oplabs-tools-data.api_table_uploads.dune_all_gas")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "oplabs-tools-data.dailydata_coingecko.coingecko_daily_prices_v1",
    "group_id": "coingecko_daily_prices_v1_table_62e8ff23",
    "columns": [
        "dt",
        "last_updated",
        "market_cap_usd",
        "price_usd",
        "token_id",
        "total_volume_usd"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def coingecko_daily_prices_v1_table_62e8ff23(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.dailydata_coingecko.coingecko_daily_prices_v1\nColumns: 6"""
    context.log.info("Processing base table: oplabs-tools-data.dailydata_coingecko.coingecko_daily_prices_v1")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "api_table_uploads.dune_all_txs",
    "group_id": "dune_all_txs_table_2bf9dc2d",
    "columns": [
        "blockchain",
        "chain_id",
        "display_name",
        "dt",
        "last_updated",
        "median_tx_fee_native",
        "median_tx_fee_usd",
        "num_blocks",
        "num_txs",
        "source",
        "tx_fee_currency",
        "tx_fee_native",
        "tx_fee_usd"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def dune_all_txs_table_2bf9dc2d(context: OpExecutionContext):
    """Base table column group: api_table_uploads.dune_all_txs\nColumns: 13"""
    context.log.info("Processing base table: api_table_uploads.dune_all_txs")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "oplabs-tools-data.api_table_uploads.daily_op_stack_chains_l1_data",
    "group_id": "daily_op_stack_chains_l1_data_table_f3f082a4",
    "columns": [
        "alignment_map",
        "avg_blob_base_fee_on_l1_inbox",
        "avg_l1_calldata_gas_price_on_l1_inbox",
        "avg_l1_gas_price_on_l1_fp_resolve",
        "avg_l1_gas_price_on_l1_inbox",
        "avg_l1_gas_price_on_l1_output",
        "avg_secs_between_l1_txs_inbox",
        "avg_secs_between_l1_txs_output",
        "calldata_bytes_l1_inbox",
        "chain_id",
        "chain_version",
        "data_availability_data_source",
        "data_availability_layer",
        "dt",
        "l1_blobgas_eth_fees_inbox",
        "l1_blobgas_purchased_inbox",
        "l1_blobgas_usd_fees_inbox",
        "l1_calldata_eth_fees_inbox",
        "l1_calldata_usd_fees_inbox",
        "l1_eth_fees_combined",
        "l1_eth_fees_fp_resolve",
        "l1_eth_fees_inbox",
        "l1_eth_fees_output",
        "l1_gas_used_combined",
        "l1_gas_used_fp_resolve",
        "l1_gas_used_inbox",
        "l1_gas_used_output",
        "l1_overhead_eth_fees_inbox",
        "l1_overhead_usd_fees_inbox",
        "l1_usd_fees_combined",
        "l1_usd_fees_fp_resolve",
        "l1_usd_fees_inbox",
        "l1_usd_fees_output",
        "last_updated",
        "name",
        "num_blobs",
        "num_l1_submissions",
        "num_l1_txs_combined",
        "num_l1_txs_fp_resolve",
        "num_l1_txs_inbox",
        "num_l1_txs_output",
        "output_root_data_source",
        "output_root_layer",
        "share_elapsed",
        "source"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def daily_op_stack_chains_l1_data_table_f3f082a4(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.api_table_uploads.daily_op_stack_chains_l1_data\nColumns: 45"""
    context.log.info("Processing base table: oplabs-tools-data.api_table_uploads.daily_op_stack_chains_l1_data")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "oplabs-tools-data.materialized_tables",
    "group_id": "materialized_tables_table_88ea7b2b",
    "columns": [
        "created_at",
        "date",
        "dt",
        "id",
        "updated_at"
    ],
    "usage_count": 0,
    "asset_type": "materialized_table"
})
def materialized_tables_table_88ea7b2b(context: OpExecutionContext):
    """Materialized table from view: unknown\nTable: oplabs-tools-data.materialized_tables"""
    context.log.info("Processing base table: oplabs-tools-data.materialized_tables")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "oplabs-tools-data.dailydata_defillama.historical_chain_tvl_v1",
    "group_id": "historical_chain_tvl_v1_table_12115837",
    "columns": [
        "chain_name",
        "dt",
        "tvl"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def historical_chain_tvl_v1_table_12115837(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.dailydata_defillama.historical_chain_tvl_v1\nColumns: 3"""
    context.log.info("Processing base table: oplabs-tools-data.dailydata_defillama.historical_chain_tvl_v1")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "oplabs-tools-data.dailydata_defillama.lend_borrow_pools_historical_v1",
    "group_id": "lend_borrow_pools_historical_v1_table_72428060",
    "columns": [
        "apy_base",
        "apy_base_borrow",
        "apy_reward",
        "apy_reward_borrow",
        "borrow_factor",
        "borrowable",
        "chain",
        "debt_ceiling_usd",
        "dt",
        "exposure",
        "il_risk",
        "is_stablecoin",
        "minted_coin",
        "pool",
        "pool_meta",
        "protocol_slug",
        "reward_tokens",
        "symbol",
        "total_borrow_usd",
        "total_supply_usd",
        "underlying_tokens"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def lend_borrow_pools_historical_v1_table_72428060(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.dailydata_defillama.lend_borrow_pools_historical_v1\nColumns: 21"""
    context.log.info("Processing base table: oplabs-tools-data.dailydata_defillama.lend_borrow_pools_historical_v1")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "dfl_stables_data",
    "group_id": "dfl_stables_data_table_2de3f4f5",
    "columns": [
        "created_at",
        "date",
        "dt",
        "id",
        "updated_at"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def dfl_stables_data_table_2de3f4f5(context: OpExecutionContext):
    """Base table column group: dfl_stables_data\nColumns: 5"""
    context.log.info("Processing base table: dfl_stables_data")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "api_table_uploads.daily_aggegate_l2_chain_usage_goldsky",
    "group_id": "daily_aggegate_l2_chain_usage_goldsky_table_634537ed",
    "columns": [
        "active_secs_per_day",
        "avg_base_fee_gwei",
        "avg_blob_base_fee_on_l2",
        "avg_l1_blob_fee_scalar",
        "avg_l1_fee_scalar",
        "avg_l1_gas_price_on_l2",
        "avg_l2_gas_price_gwei",
        "blob_gas_paid",
        "blob_gas_paid_fjord",
        "blob_gas_paid_user_txs",
        "block_time_sec",
        "calldata_bytes_l2_per_day",
        "calldata_bytes_user_txs_l2_per_day",
        "chain",
        "chain_id",
        "chain_name",
        "chain_raw",
        "compressedtxsize_approx_l2_per_day_ecotone",
        "compressedtxsize_approx_user_txs_l2_per_day_ecotone",
        "dt",
        "equivalent_l1_tx_fee",
        "estimated_size_user_txs",
        "input_calldata_gas_l2_per_day",
        "input_calldata_gas_user_txs_l2_per_day",
        "l1_blobgas_contrib_l2_eth_fees_per_day",
        "l1_contrib_l2_eth_fees_per_day",
        "l1_gas_paid",
        "l1_gas_paid_fjord",
        "l1_gas_paid_user_txs",
        "l1_gas_used_on_l2",
        "l1_gas_used_user_txs_l2_per_day",
        "l1_l1gas_contrib_l2_eth_fees_per_day",
        "l2_contrib_l2_eth_fees_base_fee_per_day",
        "l2_contrib_l2_eth_fees_per_day",
        "l2_contrib_l2_eth_fees_priority_fee_per_day",
        "l2_eth_fees_per_block",
        "l2_eth_fees_per_day",
        "l2_eth_fees_per_second",
        "l2_gas_used",
        "l2_gas_used_per_block",
        "l2_gas_used_per_second",
        "l2_gas_used_user_txs_per_day",
        "l2_num_attr_deposit_txs_per_day",
        "l2_num_success_txs_per_day",
        "l2_num_txs_per_day",
        "l2_num_txs_per_day_per_block",
        "l2_num_user_deposit_txs_per_day",
        "median_l2_eth_fees_per_tx",
        "network",
        "num_blocks",
        "num_raw_txs",
        "num_senders_per_day",
        "num_user_txs_per_second"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def daily_aggegate_l2_chain_usage_goldsky_table_634537ed(context: OpExecutionContext):
    """Base table column group: api_table_uploads.daily_aggegate_l2_chain_usage_goldsky\nColumns: 53"""
    context.log.info("Processing base table: api_table_uploads.daily_aggegate_l2_chain_usage_goldsky")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "latest_revenue",
    "group_id": "latest_revenue_table_1cb503ad",
    "columns": [
        "created_at",
        "date",
        "dt",
        "id",
        "updated_at"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def latest_revenue_table_1cb503ad(context: OpExecutionContext):
    """Base table column group: latest_revenue\nColumns: 5"""
    context.log.info("Processing base table: latest_revenue")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "unified_eco_level",
    "group_id": "unified_eco_level_table_adf9f022",
    "columns": [
        "created_at",
        "date",
        "dt",
        "id",
        "updated_at"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def unified_eco_level_table_adf9f022(context: OpExecutionContext):
    """Base table column group: unified_eco_level\nColumns: 5"""
    context.log.info("Processing base table: unified_eco_level")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "oplabs-tools-data.dailydata_l2beat",
    "group_id": "dailydata_l2beat_table_fa99d298",
    "columns": [
        "avg",
        "count",
        "created_at",
        "date",
        "dt",
        "id",
        "sum",
        "updated_at"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def dailydata_l2beat_table_fa99d298(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.dailydata_l2beat\nColumns: 8"""
    context.log.info("Processing base table: oplabs-tools-data.dailydata_l2beat")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "api_table_uploads.daily_evms_qualified_txs_counts",
    "group_id": "daily_evms_qualified_txs_counts_table_e1669526",
    "columns": [
        "alignment",
        "blockchain",
        "chain_id",
        "chain_name",
        "display_name",
        "dt",
        "is_op_chain",
        "layer",
        "num_qualified_txs",
        "num_raw_txs",
        "num_success_txs",
        "op_based_version",
        "source",
        "sum_qualified_gas_used",
        "sum_raw_gas_used",
        "sum_success_gas_used"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def daily_evms_qualified_txs_counts_table_e1669526(context: OpExecutionContext):
    """Base table column group: api_table_uploads.daily_evms_qualified_txs_counts\nColumns: 16"""
    context.log.info("Processing base table: api_table_uploads.daily_evms_qualified_txs_counts")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "oplabs-tools-data.api_table_uploads.defillama_chain_categorization",
    "group_id": "defillama_chain_categorization_table_a24b2913",
    "columns": [
        "categories",
        "chainId",
        "cmcId",
        "defillama_slug",
        "geckoId",
        "github",
        "governanceID",
        "is_EVM",
        "is_Rollup",
        "layer",
        "parent",
        "symbol",
        "twitter",
        "url"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def defillama_chain_categorization_table_a24b2913(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.api_table_uploads.defillama_chain_categorization\nColumns: 14"""
    context.log.info("Processing base table: oplabs-tools-data.api_table_uploads.defillama_chain_categorization")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "oplabs-tools-data.rpc_table_uploads",
    "group_id": "rpc_table_uploads_table_8ade1de6",
    "columns": [
        "created_at",
        "date",
        "dt",
        "id",
        "updated_at"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def rpc_table_uploads_table_8ade1de6(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.rpc_table_uploads\nColumns: 5"""
    context.log.info("Processing base table: oplabs-tools-data.rpc_table_uploads")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "api_table_uploads.daily_growthepie_l2_activity",
    "group_id": "daily_growthepie_l2_activity_table_ec0a2bb5",
    "columns": [
        "aa_last7d",
        "app_fees_eth",
        "app_fees_usd",
        "chain_name",
        "costs_blobs_eth",
        "costs_blobs_usd",
        "costs_l1_eth",
        "costs_l1_usd",
        "costs_total_eth",
        "costs_total_usd",
        "daa",
        "dt",
        "evm_chain_id",
        "fdv_eth",
        "fdv_usd",
        "fees_paid_eth",
        "fees_paid_usd",
        "gas_per_second",
        "market_cap_eth",
        "market_cap_usd",
        "origin_key",
        "profit_eth",
        "profit_usd",
        "rent_paid_eth",
        "rent_paid_usd",
        "stables_mcap",
        "stables_mcap_eth",
        "tvl",
        "tvl_eth",
        "txcosts_median_eth",
        "txcosts_median_usd",
        "txcount"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def daily_growthepie_l2_activity_table_ec0a2bb5(context: OpExecutionContext):
    """Base table column group: api_table_uploads.daily_growthepie_l2_activity\nColumns: 32"""
    context.log.info("Processing base table: api_table_uploads.daily_growthepie_l2_activity")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "oplabs-tools-data.api_table_uploads.op_stack_chain_metadata",
    "group_id": "op_stack_chain_metadata_table_5fe3404a",
    "columns": [
        "alignment",
        "batchinbox_from",
        "batchinbox_to",
        "block_explorer_url",
        "block_time_sec",
        "cgt_coingecko_api",
        "chain_layer",
        "chain_name",
        "chain_type",
        "da_layer",
        "defillama_slug",
        "display_name",
        "dispute_game_factory",
        "dune_schema",
        "flipside_schema",
        "gas_token",
        "github_url",
        "goldsky_schema",
        "growthepie_origin_key",
        "has_mods",
        "hex_color",
        "is_op_chain",
        "l1_standard_bridge",
        "l2beat_slug",
        "mainnet_chain_id",
        "migration_start",
        "op_based_version",
        "op_chain_start",
        "op_governed_start",
        "oplabs_db_schema",
        "oplabs_testnet_db_schema",
        "optimism_portal",
        "oso_schema",
        "output_root_layer",
        "outputoracle_from",
        "outputoracle_to_proxy",
        "product_website",
        "public_mainnet_launch_date",
        "raas_deployer",
        "rpc_url",
        "system_config_proxy"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def op_stack_chain_metadata_table_5fe3404a(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.api_table_uploads.op_stack_chain_metadata\nColumns: 41"""
    context.log.info("Processing base table: oplabs-tools-data.api_table_uploads.op_stack_chain_metadata")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "api_table_uploads.daily_l2beat_l2_activity",
    "group_id": "daily_l2beat_l2_activity_table_891fbc3f",
    "columns": [
        "alignment",
        "canonicalEth",
        "canonicalUsd",
        "category",
        "chain",
        "chainId",
        "chain_name",
        "display_name",
        "ethereumTransactions",
        "externalEth",
        "externalUsd",
        "is_archived",
        "is_current_chain",
        "is_op_chain",
        "is_upcoming",
        "l2beat_slug",
        "layer",
        "mainnet_chain_id",
        "name",
        "nativeEth",
        "nativeUsd",
        "op_based_version",
        "provider",
        "provider_entity",
        "timestamp",
        "totalEth",
        "totalUsd",
        "transactions"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def daily_l2beat_l2_activity_table_891fbc3f(context: OpExecutionContext):
    """Base table column group: api_table_uploads.daily_l2beat_l2_activity\nColumns: 28"""
    context.log.info("Processing base table: api_table_uploads.daily_l2beat_l2_activity")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "oplabs-tools-data.api_table_uploads.daily_defillama_chain_tvl",
    "group_id": "daily_defillama_chain_tvl_table_5e4e0e2c",
    "columns": [
        "alignment",
        "avg_usd_tvl",
        "chain",
        "chainId",
        "chain_name",
        "date",
        "defillama_slug",
        "display_name",
        "distinct_parent_protocol",
        "distinct_protocol",
        "is_EVM",
        "is_Rollup",
        "is_op_chain",
        "layer",
        "mainnet_chain_id",
        "op_based_version",
        "source",
        "sum_token_value_usd_flow",
        "sum_token_value_usd_price_change",
        "tvl"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def daily_defillama_chain_tvl_table_5e4e0e2c(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.api_table_uploads.daily_defillama_chain_tvl\nColumns: 20"""
    context.log.info("Processing base table: oplabs-tools-data.api_table_uploads.daily_defillama_chain_tvl")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "oplabs-tools-data.dailydata_defillama.fees_protocols_metadata_v1",
    "group_id": "fees_protocols_metadata_v1_table_21bc21fe",
    "columns": [
        "category",
        "chains",
        "defillamaId",
        "displayName",
        "dt",
        "id",
        "latestFetchIsOk",
        "logo",
        "methodology",
        "methodologyURL",
        "module",
        "name",
        "parentProtocol",
        "protocolType",
        "slug"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def fees_protocols_metadata_v1_table_21bc21fe(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.dailydata_defillama.fees_protocols_metadata_v1\nColumns: 15"""
    context.log.info("Processing base table: oplabs-tools-data.dailydata_defillama.fees_protocols_metadata_v1")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "latest_fees",
    "group_id": "latest_fees_table_ba11b366",
    "columns": [
        "created_at",
        "date",
        "dt",
        "id",
        "updated_at"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def latest_fees_table_ba11b366(context: OpExecutionContext):
    """Base table column group: latest_fees\nColumns: 5"""
    context.log.info("Processing base table: latest_fees")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "latest_tvl",
    "group_id": "latest_tvl_table_89b0689d",
    "columns": [
        "created_at",
        "date",
        "dt",
        "id",
        "updated_at"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def latest_tvl_table_89b0689d(context: OpExecutionContext):
    """Base table column group: latest_tvl\nColumns: 5"""
    context.log.info("Processing base table: latest_tvl")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "oplabs-tools-data.dailydata_l2beat.chain_summary_latest",
    "group_id": "chain_summary_latest_table_a4add729",
    "columns": [
        "category",
        "da_badge",
        "hostChain",
        "id",
        "isArchived",
        "isUnderReview",
        "isUpcoming",
        "name",
        "provider",
        "purposes",
        "shortName",
        "slug",
        "stage",
        "tvl_associated",
        "tvl_associated_tokens",
        "tvl_ether",
        "tvl_stablecoin",
        "tvl_total",
        "type",
        "vm_badge"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def chain_summary_latest_table_a4add729(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.dailydata_l2beat.chain_summary_latest\nColumns: 20"""
    context.log.info("Processing base table: oplabs-tools-data.dailydata_l2beat.chain_summary_latest")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "dfl_volume_fees_data",
    "group_id": "dfl_volume_fees_data_table_fb1a2400",
    "columns": [
        "created_at",
        "date",
        "dt",
        "id",
        "updated_at"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def dfl_volume_fees_data_table_fb1a2400(context: OpExecutionContext):
    """Base table column group: dfl_volume_fees_data\nColumns: 5"""
    context.log.info("Processing base table: dfl_volume_fees_data")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "artemis-bigquery-share.optimism.ez_github_repository_ecosystems",
    "group_id": "ez_github_repository_ecosystems_table_776fb70d",
    "columns": [
        "ECOSYSTEM_NAME",
        "FORKED_FROM",
        "LAST_UPDATED",
        "REPO_FULL_NAME",
        "SYMBOL",
        "UPDATE_STATUS",
        "VERSION"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def ez_github_repository_ecosystems_table_776fb70d(context: OpExecutionContext):
    """Base table column group: artemis-bigquery-share.optimism.ez_github_repository_ecosystems\nColumns: 7"""
    context.log.info("Processing base table: artemis-bigquery-share.optimism.ez_github_repository_ecosystems")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "oplabs-tools-data.api_table_uploads.op_collective_revenue_transactions",
    "group_id": "op_collective_revenue_transactions_table_7a26032e",
    "columns": [
        "chain_id",
        "chain_name",
        "from_name",
        "is_op_transfer",
        "last_updated",
        "source",
        "tfer_from",
        "tfer_to",
        "to_name",
        "trace_address",
        "transfer_chain",
        "tx_block_number",
        "tx_block_time",
        "tx_from",
        "tx_hash",
        "tx_method_id",
        "tx_to",
        "value",
        "value_decimal"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def op_collective_revenue_transactions_table_7a26032e(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.api_table_uploads.op_collective_revenue_transactions\nColumns: 19"""
    context.log.info("Processing base table: oplabs-tools-data.api_table_uploads.op_collective_revenue_transactions")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "modern_api",
    "group_id": "modern_api_table_135bc095",
    "columns": [
        "created_at",
        "date",
        "dt",
        "id",
        "updated_at"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def modern_api_table_135bc095(context: OpExecutionContext):
    """Base table column group: modern_api\nColumns: 5"""
    context.log.info("Processing base table: modern_api")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "oplabs-tools-data.dailydata_defillama.volume_protocols_metadata_v1",
    "group_id": "volume_protocols_metadata_v1_table_d40811ed",
    "columns": [
        "category",
        "chains",
        "defillamaId",
        "displayName",
        "dt",
        "id",
        "latestFetchIsOk",
        "logo",
        "methodology",
        "methodologyURL",
        "module",
        "name",
        "parentProtocol",
        "protocolType",
        "slug"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def volume_protocols_metadata_v1_table_d40811ed(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.dailydata_defillama.volume_protocols_metadata_v1\nColumns: 15"""
    context.log.info("Processing base table: oplabs-tools-data.dailydata_defillama.volume_protocols_metadata_v1")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "oplabs-tools-data.dailydata_defillama.defillama_tvl_breakdown_filtered",
    "group_id": "defillama_tvl_breakdown_filtered_table_9b501e5c",
    "columns": [
        "alignment",
        "app_token_tvl",
        "app_token_tvl_14d",
        "app_token_tvl_1d",
        "app_token_tvl_28d",
        "app_token_tvl_365d",
        "app_token_tvl_60d",
        "app_token_tvl_7d",
        "app_token_tvl_90d",
        "app_token_tvl_usd",
        "chain",
        "dt",
        "is_double_counted",
        "is_protocol_misrepresented",
        "misrepresented_tokens",
        "net_token_flow_14d",
        "net_token_flow_1d",
        "net_token_flow_28d",
        "net_token_flow_365d",
        "net_token_flow_60d",
        "net_token_flow_7d",
        "net_token_flow_90d",
        "parent_protocol",
        "protocol_category",
        "protocol_name",
        "protocol_slug",
        "to_filter_out",
        "token",
        "token_category",
        "token_source_project",
        "token_source_protocol",
        "usd_conversion_rate"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def defillama_tvl_breakdown_filtered_table_9b501e5c(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.dailydata_defillama.defillama_tvl_breakdown_filtered\nColumns: 32"""
    context.log.info("Processing base table: oplabs-tools-data.dailydata_defillama.defillama_tvl_breakdown_filtered")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "joined_ecosystems",
    "group_id": "joined_ecosystems_table_d0f8c5d3",
    "columns": [
        "created_at",
        "date",
        "dt",
        "id",
        "updated_at"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def joined_ecosystems_table_d0f8c5d3(context: OpExecutionContext):
    """Base table column group: joined_ecosystems\nColumns: 5"""
    context.log.info("Processing base table: joined_ecosystems")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "oplabs-tools-data.api_table_uploads.l2beat_l2_metadata",
    "group_id": "l2beat_l2_metadata_table_3ca16471",
    "columns": [
        "badges",
        "category",
        "chain",
        "chainId",
        "da_provider_name",
        "documentation",
        "explorerUrl",
        "file_name",
        "hostChain",
        "isArchived",
        "is_archived",
        "is_current_chain",
        "is_upcoming",
        "layer",
        "name",
        "provider",
        "provider_entity",
        "repositories",
        "rpcUrl",
        "slug",
        "websites"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def l2beat_l2_metadata_table_3ca16471(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.api_table_uploads.l2beat_l2_metadata\nColumns: 21"""
    context.log.info("Processing base table: oplabs-tools-data.api_table_uploads.l2beat_l2_metadata")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "dailydata_l2beat.tvl_history",
    "group_id": "tvl_history_table_e3de8abd",
    "columns": [
        "canonical",
        "dt",
        "ethPrice",
        "external",
        "id",
        "native",
        "slug",
        "timestamp"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def tvl_history_table_e3de8abd(context: OpExecutionContext):
    """Base table column group: dailydata_l2beat.tvl_history\nColumns: 8"""
    context.log.info("Processing base table: dailydata_l2beat.tvl_history")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "dailydata_l2beat.activity_history",
    "group_id": "activity_history_table_1dc2da3d",
    "columns": [
        "dt",
        "id",
        "slug",
        "timestamp",
        "transaction_count",
        "userops_count"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def activity_history_table_1dc2da3d(context: OpExecutionContext):
    """Base table column group: dailydata_l2beat.activity_history\nColumns: 6"""
    context.log.info("Processing base table: dailydata_l2beat.activity_history")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "oplabs-tools-data.uploads_api",
    "group_id": "uploads_api_table_9599d34c",
    "columns": [
        "created_at",
        "date",
        "dt",
        "id",
        "updated_at"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def uploads_api_table_9599d34c(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.uploads_api\nColumns: 5"""
    context.log.info("Processing base table: oplabs-tools-data.uploads_api")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "api_table_uploads",
    "group_id": "api_table_uploads_table_23681400",
    "columns": [
        "created_at",
        "date",
        "dt",
        "id",
        "updated_at"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def api_table_uploads_table_23681400(context: OpExecutionContext):
    """Base table column group: api_table_uploads\nColumns: 5"""
    context.log.info("Processing base table: api_table_uploads")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "api_table_uploads.op_stack_chain_metadata",
    "group_id": "op_stack_chain_metadata_table_ea85dd2a",
    "columns": [
        "alignment",
        "batchinbox_from",
        "batchinbox_to",
        "block_explorer_url",
        "block_time_sec",
        "cgt_coingecko_api",
        "chain_layer",
        "chain_name",
        "chain_type",
        "da_layer",
        "defillama_slug",
        "display_name",
        "dispute_game_factory",
        "dune_schema",
        "flipside_schema",
        "gas_token",
        "github_url",
        "goldsky_schema",
        "growthepie_origin_key",
        "has_mods",
        "hex_color",
        "is_op_chain",
        "l1_standard_bridge",
        "l2beat_slug",
        "mainnet_chain_id",
        "migration_start",
        "op_based_version",
        "op_chain_start",
        "op_governed_start",
        "oplabs_db_schema",
        "oplabs_testnet_db_schema",
        "optimism_portal",
        "oso_schema",
        "output_root_layer",
        "outputoracle_from",
        "outputoracle_to_proxy",
        "product_website",
        "public_mainnet_launch_date",
        "raas_deployer",
        "rpc_url",
        "system_config_proxy"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def op_stack_chain_metadata_table_ea85dd2a(context: OpExecutionContext):
    """Base table column group: api_table_uploads.op_stack_chain_metadata\nColumns: 41"""
    context.log.info("Processing base table: api_table_uploads.op_stack_chain_metadata")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "oplabs-tools-data.dailydata_defillama.volume_fees_revenue_breakdown_v1",
    "group_id": "dailydata_defillama_table_9b1f5555",
    "columns": [
        "avg",
        "count",
        "created_at",
        "date",
        "dt",
        "id",
        "sum",
        "updated_at"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def dailydata_defillama_table_9b1f5555(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.dailydata_defillama\nColumns: 8"""
    context.log.info("Processing base table: oplabs-tools-data.dailydata_defillama")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "artemis-bigquery-share.optimism.ez_github_repository_subecosystems",
    "group_id": "ez_github_repository_subecosystems_table_a20f6518",
    "columns": [
        "ECOSYSTEM_NAME",
        "SUBECOSYSTEM_NAME"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def ez_github_repository_subecosystems_table_a20f6518(context: OpExecutionContext):
    """Base table column group: artemis-bigquery-share.optimism.ez_github_repository_subecosystems\nColumns: 2"""
    context.log.info("Processing base table: artemis-bigquery-share.optimism.ez_github_repository_subecosystems")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "dfl_lend_borrow",
    "group_id": "dfl_lend_borrow_table_0008df31",
    "columns": [
        "created_at",
        "date",
        "dt",
        "id",
        "updated_at"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def dfl_lend_borrow_table_0008df31(context: OpExecutionContext):
    """Base table column group: dfl_lend_borrow\nColumns: 5"""
    context.log.info("Processing base table: dfl_lend_borrow")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "oplabs-tools-data.dailydata_defillama.stablecoins_metadata_v1",
    "group_id": "stablecoins_metadata_v1_table_448e27a3",
    "columns": [
        "address",
        "cmcId",
        "description",
        "dt",
        "gecko_id",
        "id",
        "mintRedeemDescription",
        "name",
        "onCoinGecko",
        "pegMechanism",
        "pegType",
        "price",
        "priceSource",
        "symbol",
        "twitter",
        "url"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def stablecoins_metadata_v1_table_448e27a3(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.dailydata_defillama.stablecoins_metadata_v1\nColumns: 16"""
    context.log.info("Processing base table: oplabs-tools-data.dailydata_defillama.stablecoins_metadata_v1")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "tvl_associated_tokens",
    "group_id": "tvl_associated_tokens_table_a753dafa",
    "columns": [
        "created_at",
        "date",
        "dt",
        "id",
        "updated_at"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def tvl_associated_tokens_table_a753dafa(context: OpExecutionContext):
    """Base table column group: tvl_associated_tokens\nColumns: 5"""
    context.log.info("Processing base table: tvl_associated_tokens")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "oplabs-tools-data.dailydata_coingecko.coingecko_token_metadata_v1_latest",
    "group_id": "coingecko_token_metadata_v1_latest_table_21e5481d",
    "columns": [
        "announcement_url",
        "blockchain_site",
        "categories",
        "chat_url",
        "contract_addresses",
        "description",
        "homepage",
        "last_updated",
        "name",
        "official_forum_url",
        "repos_url",
        "subreddit_url",
        "symbol",
        "telegram_channel_identifier",
        "token_id",
        "twitter_screen_name"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def coingecko_token_metadata_v1_latest_table_21e5481d(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.dailydata_coingecko.coingecko_token_metadata_v1_latest\nColumns: 16"""
    context.log.info("Processing base table: oplabs-tools-data.dailydata_coingecko.coingecko_token_metadata_v1_latest")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "oplabs-tools-data.api_table_uploads",
    "group_id": "api_table_uploads_table_2fa2488e",
    "columns": [
        "created_at",
        "date",
        "dt",
        "id",
        "updated_at"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def api_table_uploads_table_2fa2488e(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.api_table_uploads\nColumns: 5"""
    context.log.info("Processing base table: oplabs-tools-data.api_table_uploads")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "oplabs-tools-data.views",
    "group_id": "views_table_24f68cb4",
    "columns": [
        "created_at",
        "date",
        "dt",
        "id",
        "updated_at"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def views_table_24f68cb4(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.views\nColumns: 5"""
    context.log.info("Processing base table: oplabs-tools-data.views")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "oplabs-tools-data.dailydata_defillama.protocols_tvl_v1",
    "group_id": "protocols_tvl_v1_table_2b01f485",
    "columns": [
        "chain",
        "dt",
        "protocol_slug",
        "total_app_tvl"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def protocols_tvl_v1_table_2b01f485(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.dailydata_defillama.protocols_tvl_v1\nColumns: 4"""
    context.log.info("Processing base table: oplabs-tools-data.dailydata_defillama.protocols_tvl_v1")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "oplabs-tools-data.gcs_uploads_api.stablecoins_metadata_v1",
    "group_id": "stablecoins_metadata_v1_table_03e6ece5",
    "columns": [
        "address",
        "cmcId",
        "description",
        "dt",
        "gecko_id",
        "id",
        "mintRedeemDescription",
        "name",
        "onCoinGecko",
        "pegMechanism",
        "pegType",
        "price",
        "priceSource",
        "symbol",
        "twitter",
        "url"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def stablecoins_metadata_v1_table_03e6ece5(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.gcs_uploads_api.stablecoins_metadata_v1\nColumns: 16"""
    context.log.info("Processing base table: oplabs-tools-data.gcs_uploads_api.stablecoins_metadata_v1")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "artemis-bigquery-share.optimism",
    "group_id": "optimism_table_09a25bb1",
    "columns": [
        "created_at",
        "date",
        "dt",
        "id",
        "updated_at"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def optimism_table_09a25bb1(context: OpExecutionContext):
    """Base table column group: artemis-bigquery-share.optimism\nColumns: 5"""
    context.log.info("Processing base table: artemis-bigquery-share.optimism")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "oplabs-tools-data.dailydata_defillama.revenue_protocols_metadata_v1",
    "group_id": "revenue_protocols_metadata_v1_table_bcb20f67",
    "columns": [
        "category",
        "chains",
        "defillamaId",
        "displayName",
        "dt",
        "id",
        "latestFetchIsOk",
        "logo",
        "methodology",
        "methodologyURL",
        "module",
        "name",
        "parentProtocol",
        "protocolType",
        "slug"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def revenue_protocols_metadata_v1_table_bcb20f67(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.dailydata_defillama.revenue_protocols_metadata_v1\nColumns: 15"""
    context.log.info("Processing base table: oplabs-tools-data.dailydata_defillama.revenue_protocols_metadata_v1")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "oplabs-tools-data.dailydata_coingecko",
    "group_id": "dailydata_coingecko_table_af851656",
    "columns": [
        "avg",
        "count",
        "created_at",
        "date",
        "dt",
        "id",
        "sum",
        "updated_at"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def dailydata_coingecko_table_af851656(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.dailydata_coingecko\nColumns: 8"""
    context.log.info("Processing base table: oplabs-tools-data.dailydata_coingecko")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "oplabs-tools-data.api_table_uploads.dune_all_txs",
    "group_id": "dune_all_txs_table_eaa13223",
    "columns": [
        "blockchain",
        "chain_id",
        "display_name",
        "dt",
        "last_updated",
        "median_tx_fee_native",
        "median_tx_fee_usd",
        "num_blocks",
        "num_txs",
        "source",
        "tx_fee_currency",
        "tx_fee_native",
        "tx_fee_usd"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def dune_all_txs_table_eaa13223(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.api_table_uploads.dune_all_txs\nColumns: 13"""
    context.log.info("Processing base table: oplabs-tools-data.api_table_uploads.dune_all_txs")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "grouped_metadata",
    "group_id": "grouped_metadata_table_73ca4b8f",
    "columns": [
        "created_at",
        "date",
        "description",
        "dt",
        "id",
        "name",
        "type",
        "updated_at"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def grouped_metadata_table_73ca4b8f(context: OpExecutionContext):
    """Base table column group: grouped_metadata\nColumns: 8"""
    context.log.info("Processing base table: grouped_metadata")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "api_table_uploads.growthepie_l2_metadata",
    "group_id": "growthepie_l2_metadata_table_c34fabe0",
    "columns": [
        "block_explorer",
        "block_explorers",
        "bucket",
        "caip2",
        "chain_name",
        "chain_type",
        "colors",
        "company",
        "da_layer",
        "deployment",
        "description",
        "ecosystem",
        "enable_contracts",
        "evm_chain_id",
        "l2beat_id",
        "l2beat_link",
        "l2beat_stage",
        "launch_date",
        "logo",
        "maturity",
        "name",
        "origin_key",
        "purpose",
        "raas",
        "rhino_listed",
        "rhino_naming",
        "stack",
        "symbol",
        "technology",
        "twitter",
        "url_key",
        "website"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def growthepie_l2_metadata_table_c34fabe0(context: OpExecutionContext):
    """Base table column group: api_table_uploads.growthepie_l2_metadata\nColumns: 32"""
    context.log.info("Processing base table: api_table_uploads.growthepie_l2_metadata")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "oplabs-tools-data.api_table_uploads.dune_ovm1_daily_data",
    "group_id": "dune_ovm1_daily_data_table_62cca5ab",
    "columns": [
        "active_secs_per_day",
        "avg_blob_gas_price_on_l2",
        "avg_l1_fee_scalar",
        "avg_l1_gas_price_on_l2",
        "avg_l2_gas_price",
        "base_fee_gwei",
        "blockchain",
        "calldata_bytes_l2_per_day",
        "calldata_bytes_user_txs_l2_per_day",
        "calldata_gas_l2_per_day",
        "calldata_gas_user_txs_l2_per_day",
        "chain_id",
        "compressedtxsize_approx_l2_per_day",
        "compressedtxsize_approx_user_txs_l2_per_day",
        "dt",
        "equivalent_l1_tx_fee",
        "l1_contrib_l2_eth_fees_per_day",
        "l1_contrib_l2_usd_fees_per_day",
        "l1_gas_used_on_l2",
        "l1_gas_used_user_txs_l2_per_day",
        "l2_contrib_l2_eth_fees_per_day",
        "l2_contrib_l2_usd_fees_per_day",
        "l2_data_source",
        "l2_gas_eth_fees_base_fee",
        "l2_gas_eth_fees_priority_fee",
        "l2_gas_usd_fees_base_fee",
        "l2_gas_usd_fees_priority_fee",
        "l2_gas_used",
        "l2_gas_used_user_txs_per_day",
        "l2_num_attr_deposit_txs_per_day",
        "l2_num_success_txs_per_day",
        "l2_num_txs_per_day",
        "l2_num_user_deposit_txs_per_day",
        "last_updated",
        "median_l2_eth_fees_per_tx",
        "num_blocks",
        "num_users_per_day",
        "source"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def dune_ovm1_daily_data_table_62cca5ab(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.api_table_uploads.dune_ovm1_daily_data\nColumns: 38"""
    context.log.info("Processing base table: oplabs-tools-data.api_table_uploads.dune_ovm1_daily_data")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "oplabs-tools-data.gcs_uploads_api",
    "group_id": "gcs_uploads_api_table_1838c530",
    "columns": [
        "created_at",
        "date",
        "dt",
        "id",
        "updated_at"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def gcs_uploads_api_table_1838c530(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.gcs_uploads_api\nColumns: 5"""
    context.log.info("Processing base table: oplabs-tools-data.gcs_uploads_api")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "unique_data",
    "group_id": "unique_data_table_6ae8d8fa",
    "columns": [
        "created_at",
        "date",
        "dt",
        "id",
        "updated_at"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def unique_data_table_6ae8d8fa(context: OpExecutionContext):
    """Base table column group: unique_data\nColumns: 5"""
    context.log.info("Processing base table: unique_data")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "dailydata_l2beat.chain_summary_latest",
    "group_id": "chain_summary_latest_table_19755cb3",
    "columns": [
        "category",
        "da_badge",
        "hostChain",
        "id",
        "isArchived",
        "isUnderReview",
        "isUpcoming",
        "name",
        "provider",
        "purposes",
        "shortName",
        "slug",
        "stage",
        "tvl_associated",
        "tvl_associated_tokens",
        "tvl_ether",
        "tvl_stablecoin",
        "tvl_total",
        "type",
        "vm_badge"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def chain_summary_latest_table_19755cb3(context: OpExecutionContext):
    """Base table column group: dailydata_l2beat.chain_summary_latest\nColumns: 20"""
    context.log.info("Processing base table: dailydata_l2beat.chain_summary_latest")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "oplabs-tools-data.rpc_table_uploads.market_data",
    "group_id": "market_data_table_99ab9072",
    "columns": [
        "blob_base_fee_gwei",
        "eth_usd",
        "l1_base_fee_gwei",
        "timestamp"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def market_data_table_99ab9072(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.rpc_table_uploads.market_data\nColumns: 4"""
    context.log.info("Processing base table: oplabs-tools-data.rpc_table_uploads.market_data")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "oplabs-tools-data.api_table_uploads.dune_all_fees",
    "group_id": "dune_all_fees_table_f3b22c56",
    "columns": [
        "blockchain",
        "chain_id",
        "display_name",
        "dt",
        "last_updated",
        "tx_fee_usd"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def dune_all_fees_table_f3b22c56(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.api_table_uploads.dune_all_fees\nColumns: 6"""
    context.log.info("Processing base table: oplabs-tools-data.api_table_uploads.dune_all_fees")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "author_eco_level",
    "group_id": "author_eco_level_table_f90b92fa",
    "columns": [
        "created_at",
        "date",
        "dt",
        "id",
        "updated_at"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def author_eco_level_table_f90b92fa(context: OpExecutionContext):
    """Base table column group: author_eco_level\nColumns: 5"""
    context.log.info("Processing base table: author_eco_level")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "api_table_uploads.dune_evms_info",
    "group_id": "dune_evms_info_table_277569dd",
    "columns": [
        "chain_id",
        "chain_type",
        "codebase",
        "data_availability",
        "dune_schema",
        "explorer_link",
        "first_block_time",
        "last_updated",
        "layer",
        "name",
        "native_token_symbol",
        "placeholder",
        "rollup_type"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def dune_evms_info_table_277569dd(context: OpExecutionContext):
    """Base table column group: api_table_uploads.dune_evms_info\nColumns: 13"""
    context.log.info("Processing base table: api_table_uploads.dune_evms_info")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "oplabs-tools-data.gcs_uploads_api.daily_eth_price_volatility",
    "group_id": "daily_eth_price_volatility_table_daf22244",
    "columns": [
        "avg",
        "close",
        "dt",
        "high",
        "hl_abs_change",
        "low",
        "med",
        "open",
        "realized_volatility",
        "symbol"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def daily_eth_price_volatility_table_daf22244(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.gcs_uploads_api.daily_eth_price_volatility\nColumns: 10"""
    context.log.info("Processing base table: oplabs-tools-data.gcs_uploads_api.daily_eth_price_volatility")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "oplabs-tools-data.materialized_tables.weekly_github_author_counts_mv",
    "group_id": "weekly_github_author_counts_mv_table_8bad5631",
    "columns": [
        "MAPPED_ECOSYSTEM",
        "NUM_AUTHOR_ID",
        "START_OF_WEEK",
        "eco_type"
    ],
    "usage_count": 0,
    "asset_type": "materialized_table",
    "source_view": "oplabs-tools-data.views.weekly_github_author_counts"
})
def weekly_github_author_counts_mv_table_8bad5631(context: OpExecutionContext):
    """Materialized table from view: oplabs-tools-data.views.weekly_github_author_counts\nTable: oplabs-tools-data.materialized_tables.weekly_github_author_counts_mv"""
    context.log.info("Processing base table: oplabs-tools-data.materialized_tables.weekly_github_author_counts_mv")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "oplabs-tools-data.gcs_uploads_api.stablecoins_balances_v1",
    "group_id": "stablecoins_balances_v1_table_77f8a27b",
    "columns": [
        "bridged_to",
        "chain",
        "circulating",
        "dt",
        "id",
        "minted",
        "name",
        "symbol",
        "unreleased"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def stablecoins_balances_v1_table_77f8a27b(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.gcs_uploads_api.stablecoins_balances_v1\nColumns: 9"""
    context.log.info("Processing base table: oplabs-tools-data.gcs_uploads_api.stablecoins_balances_v1")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "dailydata_l2beat",
    "group_id": "dailydata_l2beat_table_9ed8e8b7",
    "columns": [
        "avg",
        "count",
        "created_at",
        "date",
        "dt",
        "id",
        "sum",
        "updated_at"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def dailydata_l2beat_table_9ed8e8b7(context: OpExecutionContext):
    """Base table column group: dailydata_l2beat\nColumns: 8"""
    context.log.info("Processing base table: dailydata_l2beat")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "oplabs-tools-data.api_table_uploads.daily_aggegate_l2_chain_usage_goldsky",
    "group_id": "daily_aggegate_l2_chain_usage_goldsky_table_9462e9e2",
    "columns": [
        "active_secs_per_day",
        "avg_base_fee_gwei",
        "avg_blob_base_fee_on_l2",
        "avg_l1_blob_fee_scalar",
        "avg_l1_fee_scalar",
        "avg_l1_gas_price_on_l2",
        "avg_l2_gas_price_gwei",
        "blob_gas_paid",
        "blob_gas_paid_fjord",
        "blob_gas_paid_user_txs",
        "block_time_sec",
        "calldata_bytes_l2_per_day",
        "calldata_bytes_user_txs_l2_per_day",
        "chain",
        "chain_id",
        "chain_name",
        "chain_raw",
        "compressedtxsize_approx_l2_per_day_ecotone",
        "compressedtxsize_approx_user_txs_l2_per_day_ecotone",
        "dt",
        "equivalent_l1_tx_fee",
        "estimated_size_user_txs",
        "input_calldata_gas_l2_per_day",
        "input_calldata_gas_user_txs_l2_per_day",
        "l1_blobgas_contrib_l2_eth_fees_per_day",
        "l1_contrib_l2_eth_fees_per_day",
        "l1_gas_paid",
        "l1_gas_paid_fjord",
        "l1_gas_paid_user_txs",
        "l1_gas_used_on_l2",
        "l1_gas_used_user_txs_l2_per_day",
        "l1_l1gas_contrib_l2_eth_fees_per_day",
        "l2_contrib_l2_eth_fees_base_fee_per_day",
        "l2_contrib_l2_eth_fees_per_day",
        "l2_contrib_l2_eth_fees_priority_fee_per_day",
        "l2_eth_fees_per_block",
        "l2_eth_fees_per_day",
        "l2_eth_fees_per_second",
        "l2_gas_used",
        "l2_gas_used_per_block",
        "l2_gas_used_per_second",
        "l2_gas_used_user_txs_per_day",
        "l2_num_attr_deposit_txs_per_day",
        "l2_num_success_txs_per_day",
        "l2_num_txs_per_day",
        "l2_num_txs_per_day_per_block",
        "l2_num_user_deposit_txs_per_day",
        "median_l2_eth_fees_per_tx",
        "network",
        "num_blocks",
        "num_raw_txs",
        "num_senders_per_day",
        "num_user_txs_per_second"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def daily_aggegate_l2_chain_usage_goldsky_table_9462e9e2(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.api_table_uploads.daily_aggegate_l2_chain_usage_goldsky\nColumns: 53"""
    context.log.info("Processing base table: oplabs-tools-data.api_table_uploads.daily_aggegate_l2_chain_usage_goldsky")
    # Base table asset implementation would go here


@asset(metadata={
    "table_name": "dfl_type_data",
    "group_id": "dfl_type_data_table_e1517162",
    "columns": [
        "created_at",
        "date",
        "dt",
        "id",
        "updated_at"
    ],
    "usage_count": 0,
    "asset_type": "base_table"
})
def dfl_type_data_table_e1517162(context: OpExecutionContext):
    """Base table column group: dfl_type_data\nColumns: 5"""
    context.log.info("Processing base table: dfl_type_data")
    # Base table asset implementation would go here


# =======================================================================
# View Assets (Hierarchical Dependencies)
# =======================================================================

@asset(deps=[coingecko_token_metadata_v1_latest_table_21e5481d, dailydata_coingecko_table_af851656,
             coingecko_daily_prices_v1_table_62e8ff23], metadata={
    "view_name": "view_coingecko_daily_prices",
    "asset_id": "view_coingecko_daily_prices",
    "direct_dependencies_count": 3,
    "dependents_count": 0,
    "node_type": "view",
    "asset_type": "view"
})
def view_coingecko_daily_prices(context: OpExecutionContext):
    """
View: view_coingecko_daily_prices
Type: view
Direct dependencies: 3
Used by: 0 assets
"""
    context.log.info("Processing view: view_coingecko_daily_prices")
    # View implementation would query BigQuery and return results


@asset(deps=[volume_fees_revenue_breakdown_v1_table_4e626584, dailydata_defillama_table_9b1f5555], metadata={
    "view_name": "view_defillama_vfr_28d_annualized",
    "asset_id": "view_defillama_vfr_28d_annualized",
    "direct_dependencies_count": 2,
    "dependents_count": 0,
    "node_type": "view",
    "asset_type": "view"
})
def view_defillama_vfr_28d_annualized(context: OpExecutionContext):
    """
View: view_defillama_vfr_28d_annualized
Type: view
Direct dependencies: 2
Used by: 0 assets
"""
    context.log.info("Processing view: view_defillama_vfr_28d_annualized")
    # View implementation would query BigQuery and return results


@asset(deps=[stablecoins_metadata_v1_table_448e27a3, dailydata_defillama_table_9b1f5555], metadata={
    "view_name": "view_stablecoins_metadata_v1_latest_price",
    "asset_id": "view_stablecoins_metadata_v1_latest_price",
    "direct_dependencies_count": 2,
    "dependents_count": 0,
    "node_type": "view",
    "asset_type": "view"
})
def view_stablecoins_metadata_v1_latest_price(context: OpExecutionContext):
    """
View: view_stablecoins_metadata_v1_latest_price
Type: view
Direct dependencies: 2
Used by: 0 assets
"""
    context.log.info("Processing view: view_stablecoins_metadata_v1_latest_price")
    # View implementation would query BigQuery and return results


@asset(deps=[dailydata_defillama_table_9b1f5555, latest_tvl_table_89b0689d, latest_revenue_table_1cb503ad,
             volume_protocols_metadata_v1_table_d40811ed, latest_fees_table_ba11b366,
             fees_protocols_metadata_v1_table_21bc21fe, protocols_metadata_v1_table_170c024c,
             revenue_protocols_metadata_v1_table_bcb20f67], metadata={
    "view_name": "view_latest_defillama_dexs_protocols_metadata_v1",
    "asset_id": "view_latest_defillama_dexs_protocols_metadata_v1",
    "direct_dependencies_count": 8,
    "dependents_count": 0,
    "node_type": "view",
    "asset_type": "view"
})
def view_latest_defillama_dexs_protocols_metadata_v1(context: OpExecutionContext):
    """
View: view_latest_defillama_dexs_protocols_metadata_v1
Type: view
Direct dependencies: 8
Used by: 0 assets
"""
    context.log.info("Processing view: view_latest_defillama_dexs_protocols_metadata_v1")
    # View implementation would query BigQuery and return results


@asset(deps=[rpc_table_uploads_table_8ade1de6, market_data_table_99ab9072, dailydata_coingecko_table_af851656,
             coingecko_daily_prices_v1_table_62e8ff23, daily_eth_price_volatility_table_daf22244,
             gcs_uploads_api_table_1838c530], metadata={
    "view_name": "view_daily_market_data",
    "asset_id": "view_daily_market_data",
    "direct_dependencies_count": 6,
    "dependents_count": 0,
    "node_type": "view",
    "asset_type": "view"
})
def view_daily_market_data(context: OpExecutionContext):
    """
View: view_daily_market_data
Type: view
Direct dependencies: 6
Used by: 0 assets
"""
    context.log.info("Processing view: view_daily_market_data")
    # View implementation would query BigQuery and return results


@asset(deps=[l2beat_l2_metadata_table_3ca16471, api_table_uploads_table_2fa2488e, tvl_associated_tokens_table_a753dafa,
             chain_summary_latest_table_19755cb3, chain_summary_latest_table_a4add729, dailydata_l2beat_table_fa99d298],
       metadata={
           "view_name": "view_l2beat_metadata_extended",
           "asset_id": "view_l2beat_metadata_extended",
           "direct_dependencies_count": 6,
           "dependents_count": 0,
           "node_type": "view",
           "asset_type": "view"
       })
def view_l2beat_metadata_extended(context: OpExecutionContext):
    """
View: view_l2beat_metadata_extended
Type: view
Direct dependencies: 6
Used by: 0 assets
"""
    context.log.info("Processing view: view_l2beat_metadata_extended")
    # View implementation would query BigQuery and return results


@asset(deps=[protocols_tvl_v1_table_2b01f485, api_table_uploads_table_23681400, op_stack_chain_metadata_table_ea85dd2a,
             dailydata_defillama_table_9b1f5555, api_table_uploads_table_2fa2488e,
             defillama_chain_categorization_table_a24b2913, volume_protocols_metadata_v1_table_d40811ed,
             op_stack_chain_metadata_table_5fe3404a, views_table_24f68cb4,
             defillama_tvl_breakdown_filtered_table_9b501e5c, fees_protocols_metadata_v1_table_21bc21fe,
             protocols_metadata_v1_table_170c024c, unique_data_table_6ae8d8fa,
             revenue_protocols_metadata_v1_table_bcb20f67], metadata={
    "view_name": "view_defillama_daily_distinct_protocols",
    "asset_id": "view_defillama_daily_distinct_protocols",
    "direct_dependencies_count": 14,
    "dependents_count": 0,
    "node_type": "view",
    "asset_type": "view"
})
def view_defillama_daily_distinct_protocols(context: OpExecutionContext):
    """
View: view_defillama_daily_distinct_protocols
Type: view
Direct dependencies: 14
Used by: 0 assets
"""
    context.log.info("Processing view: view_defillama_daily_distinct_protocols")
    # View implementation would query BigQuery and return results


@asset(deps=[view_defillama_daily_distinct_protocols], metadata={
    "table_name": "oplabs-tools-data.materialized_tables.defillama_daily_distinct_protocols_mv",
    "group_id": "defillama_daily_distinct_protocols_mv_table_6cd56d61",
    "columns": [
        "alignment",
        "chain",
        "chain_id",
        "chain_key",
        "da_layer",
        "dfl_chain_key",
        "display_name",
        "dt",
        "eth_eco_l2",
        "eth_eco_l2l3",
        "gas_token",
        "is_evm",
        "is_op_governed",
        "l2b_stage",
        "layer",
        "output_root_layer",
        "parent_protocol",
        "parent_protocol_display_name",
        "protocol_display_name",
        "provider",
        "provider_entity",
        "provider_entity_w_superchain",
        "slug"
    ],
    "usage_count": 0,
    "asset_type": "materialized_table",
    "source_view": "oplabs-tools-data.views.defillama_daily_distinct_protocols"
})
def defillama_daily_distinct_protocols_mv_table_6cd56d61(context: OpExecutionContext):
    """Materialized table from view: oplabs-tools-data.views.defillama_daily_distinct_protocols\nTable: oplabs-tools-data.materialized_tables.defillama_daily_distinct_protocols_mv"""
    context.log.info(
        "Processing base table: oplabs-tools-data.materialized_tables.defillama_daily_distinct_protocols_mv")
    # Base table asset implementation would go here


@asset(deps=[views_table_24f68cb4, op_stack_chain_metadata_table_ea85dd2a, op_stack_chain_metadata_table_5fe3404a,
             api_table_uploads_table_2fa2488e], metadata={
    "view_name": "view_daily_opcollective_revshare",
    "asset_id": "view_daily_opcollective_revshare",
    "direct_dependencies_count": 4,
    "dependents_count": 0,
    "node_type": "view",
    "asset_type": "view"
})
def view_daily_opcollective_revshare(context: OpExecutionContext):
    """
View: view_daily_opcollective_revshare
Type: view
Direct dependencies: 4
Used by: 0 assets
"""
    context.log.info("Processing view: view_daily_opcollective_revshare")
    # View implementation would query BigQuery and return results


@asset(deps=[uploads_api_table_9599d34c, grouped_metadata_table_73ca4b8f, api_table_uploads_table_23681400,
             op_stack_chain_metadata_table_ea85dd2a, growthepie_l2_metadata_table_c34fabe0,
             api_table_uploads_table_2fa2488e, dune_all_txs_table_2bf9dc2d, daily_growthepie_l2_activity_table_ec0a2bb5,
             daily_aggegate_l2_chain_usage_goldsky_table_9462e9e2, op_stack_chain_metadata_table_5fe3404a,
             daily_l2beat_l2_activity_table_891fbc3f, views_table_24f68cb4, daily_defillama_chain_tvl_table_5e4e0e2c,
             dune_all_txs_table_eaa13223, dune_evms_info_table_277569dd, defillama_chain_categorization_table_a24b2913,
             daily_aggegate_l2_chain_usage_goldsky_table_634537ed, view_l2beat_metadata_extended], metadata={
    "view_name": "view_all_chains_metadata",
    "asset_id": "view_all_chains_metadata",
    "direct_dependencies_count": 17,
    "dependents_count": 0,
    "node_type": "view",
    "asset_type": "view"
})
def view_all_chains_metadata(context: OpExecutionContext):
    """
View: view_all_chains_metadata
Type: view
Direct dependencies: 17
Used by: 0 assets
"""
    context.log.info("Processing view: view_all_chains_metadata")
    # View implementation would query BigQuery and return results


@asset(deps=[view_all_chains_metadata], metadata={
    "table_name": "oplabs-tools-data.materialized_tables.all_chains_metadata_mv",
    "group_id": "all_chains_metadata_mv_table_33cf8c2e",
    "columns": [
        "alignment",
        "all_chain_keys",
        "chain",
        "chain_id",
        "chain_key",
        "da_layer",
        "data_sources",
        "display_name",
        "eth_eco_l2",
        "eth_eco_l2l3",
        "gas_token",
        "is_current_chain",
        "is_evm",
        "is_upcoming",
        "l2b_da_layer",
        "l2b_stage",
        "layer",
        "max_dt_day",
        "min_dt_day",
        "op_governed_start",
        "output_root_layer",
        "provider",
        "provider_entity",
        "provider_entity_w_superchain"
    ],
    "usage_count": 0,
    "asset_type": "materialized_table"
})
def all_chains_metadata_mv_table_33cf8c2e(context: OpExecutionContext):
    """Materialized table from view: unknown\nTable: oplabs-tools-data.materialized_tables.all_chains_metadata_mv"""
    context.log.info("Processing base table: oplabs-tools-data.materialized_tables.all_chains_metadata_mv")
    # Base table asset implementation would go here


@asset(
    deps=[dune_ovm1_daily_data_table_62cca5ab, op_stack_chain_metadata_table_ea85dd2a, api_table_uploads_table_2fa2488e,
          rpc_table_uploads_table_8ade1de6, daily_op_stack_chains_l1_data_table_f3f082a4,
          hourly_cumulative_l2_revenue_snapshots_table_c63d3c58, daily_aggegate_l2_chain_usage_goldsky_table_9462e9e2,
          op_stack_chain_metadata_table_5fe3404a, views_table_24f68cb4,
          op_collective_revenue_transactions_table_7a26032e, daily_aggegate_l2_chain_usage_goldsky_table_634537ed,
          view_coingecko_daily_prices, view_daily_market_data, ],
    metadata={
        "view_name": "view_daily_opstack_chain_economics",
        "asset_id": "view_daily_opstack_chain_economics",
        "direct_dependencies_count": 11,
        "dependents_count": 0,
        "node_type": "view",
        "asset_type": "view"
    })
def view_daily_opstack_chain_economics(context: OpExecutionContext):
    """
View: view_daily_opstack_chain_economics
Type: view
Direct dependencies: 11
Used by: 0 assets
"""
    context.log.info("Processing view: view_daily_opstack_chain_economics")
    # View implementation would query BigQuery and return results


@asset(deps=[daily_aggegate_l2_chain_usage_goldsky_table_634537ed, op_stack_chain_metadata_table_ea85dd2a,
             view_daily_market_data, view_coingecko_daily_prices, view_daily_opstack_chain_economics], metadata={
    "view_name": "view_daily_op_chains_usage",
    "asset_id": "view_daily_op_chains_usage",
    "direct_dependencies_count": 0,
    "dependents_count": 0,
    "node_type": "view",
    "asset_type": "view"
})
def view_daily_op_chains_usage(context: OpExecutionContext):
    """
View: view_daily_op_chains_usage
Type: view
Direct dependencies: 0
Used by: 0 assets
"""
    context.log.info("Processing view: view_daily_op_chains_usage")
    # View implementation would query BigQuery and return results


@asset(deps=[author_eco_level_table_f90b92fa, ez_github_repository_subecosystems_table_a20f6518,
             materialized_tables_table_88ea7b2b, ez_github_developer_commits_table_549079d0, optimism_table_09a25bb1,
             joined_ecosystems_table_d0f8c5d3, unified_eco_level_table_adf9f022,
             ez_github_repository_ecosystems_table_776fb70d, all_chains_metadata_mv_table_33cf8c2e], metadata={
    "view_name": "view_weekly_github_author_counts",
    "asset_id": "view_weekly_github_author_counts",
    "direct_dependencies_count": 9,
    "dependents_count": 0,
    "node_type": "view",
    "asset_type": "view"
})
def view_weekly_github_author_counts(context: OpExecutionContext):
    """
View: view_weekly_github_author_counts
Type: view
Direct dependencies: 9
Used by: 0 assets
"""
    context.log.info("Processing view: view_weekly_github_author_counts")
    # View implementation would query BigQuery and return results


@asset(metadata={
    "view_name": "view_l2beat_daily_tvl_history",
    "asset_id": "view_l2beat_daily_tvl_history",
    "direct_dependencies_count": 0,
    "dependents_count": 0,
    "node_type": "view",
    "asset_type": "view"
})
def view_l2beat_daily_tvl_history(context: OpExecutionContext):
    """
View: view_l2beat_daily_tvl_history
Type: view
Direct dependencies: 0
Used by: 0 assets
"""
    context.log.info("Processing view: view_l2beat_daily_tvl_history")
    # View implementation would query BigQuery and return results


@asset(deps=[defillama_tvl_breakdown_filtered_table_9b501e5c, dailydata_defillama_table_9b1f5555], metadata={
    "view_name": "view_defillama_tvl_breakdown_filtered_with_min_dt",
    "asset_id": "view_defillama_tvl_breakdown_filtered_with_min_dt",
    "direct_dependencies_count": 2,
    "dependents_count": 0,
    "node_type": "view",
    "asset_type": "view"
})
def view_defillama_tvl_breakdown_filtered_with_min_dt(context: OpExecutionContext):
    """
View: view_defillama_tvl_breakdown_filtered_with_min_dt
Type: view
Direct dependencies: 2
Used by: 0 assets
"""
    context.log.info("Processing view: view_defillama_tvl_breakdown_filtered_with_min_dt")
    # View implementation would query BigQuery and return results


@asset(deps=[all_chains_metadata_mv_table_33cf8c2e, materialized_tables_table_88ea7b2b], metadata={
    "view_name": "view_all_chains_metadata_range",
    "asset_id": "view_all_chains_metadata_range",
    "direct_dependencies_count": 2,
    "dependents_count": 0,
    "node_type": "view",
    "asset_type": "view"
})
def view_all_chains_metadata_range(context: OpExecutionContext):
    """
View: view_all_chains_metadata_range
Type: view
Direct dependencies: 2
Used by: 0 assets
"""
    context.log.info("Processing view: view_all_chains_metadata_range")
    # View implementation would query BigQuery and return results


# =======================================================================
# Materialized Table Assets
# =======================================================================

@asset(deps=[view_all_chains_metadata_range], metadata={
    "table_name": "oplabs-tools-data.materialized_tables.all_chains_metadata_range_mv",
    "group_id": "all_chains_metadata_range_mv_table_d67ae33d",
    "columns": [
        "all_chain_keys",
        "chain_id",
        "chain_key",
        "da_layer",
        "data_sources",
        "display_name",
        "dt",
        "gas_token",
        "is_current_chain",
        "is_evm",
        "is_op_governed",
        "is_upcoming",
        "l2b_stage",
        "layer",
        "max_dt",
        "min_dt",
        "output_root_layer",
        "provider",
        "provider_entity",
        "provider_entity_w_superchain"
    ],
    "usage_count": 0,
    "asset_type": "materialized_table",
    "source_view": "oplabs-tools-data.views.all_chains_metadata_range"
})
def all_chains_metadata_range_mv_table_d67ae33d(context: OpExecutionContext):
    """Materialized table from view: oplabs-tools-data.views.all_chains_metadata_range\nTable: oplabs-tools-data.materialized_tables.all_chains_metadata_range_mv"""
    context.log.info("Processing base table: oplabs-tools-data.materialized_tables.all_chains_metadata_range_mv")
    # Base table asset implementation would go here


@asset(metadata={
    "materialized_table": "oplabs-tools-data.materialized_tables",
    "source_view": "unknown",
    "schedule": "unknown",
    "asset_type": "materialized_view"
})
def materialized_tables_table_88ea7b2b(context: OpExecutionContext):
    """
Materialized snapshot of view: unknown
Schedule: unknown
Target table: oplabs-tools-data.materialized_tables
"""
    context.log.info("Creating materialized snapshot: oplabs-tools-data.materialized_tables")
    # Materialized snapshot implementation would create/update the tabl


@asset(deps=[dfl_stables_data_table_2de3f4f5, dune_all_txs_table_2bf9dc2d, all_chains_metadata_range_mv_table_d67ae33d,
             daily_growthepie_l2_activity_table_ec0a2bb5, daily_l2beat_l2_activity_table_891fbc3f,
             daily_defillama_chain_tvl_table_5e4e0e2c, activity_history_table_1dc2da3d,
             chain_summary_latest_table_a4add729, dfl_lend_borrow_table_0008df31,
             lend_borrow_pools_historical_v1_table_72428060, api_table_uploads_table_23681400,
             daily_evms_qualified_txs_counts_table_e1669526, dune_all_gas_table_7416ee25,
             op_stack_chain_metadata_table_5fe3404a, materialized_tables_table_88ea7b2b, views_table_24f68cb4,
             dfl_type_data_table_e1517162, tvl_history_table_e3de8abd, defillama_chain_categorization_table_a24b2913,
             historical_chain_tvl_v1_table_12115837, gcs_uploads_api_table_1838c530, dailydata_defillama_table_9b1f5555,
             dune_all_fees_table_f3b22c56, stablecoins_balances_v1_table_77f8a27b,
             weekly_github_author_counts_mv_table_8bad5631, chain_summary_latest_table_19755cb3,
             dailydata_l2beat_table_9ed8e8b7, modern_api_table_135bc095, op_stack_chain_metadata_table_ea85dd2a,
             api_table_uploads_table_2fa2488e, stablecoins_metadata_v1_table_03e6ece5,
             dfl_volume_fees_data_table_fb1a2400, volume_fees_revenue_breakdown_v1_table_4e626584,
             defillama_daily_distinct_protocols_mv_table_6cd56d61, dune_all_txs_table_eaa13223,
             view_defillama_vfr_28d_annualized, view_defillama_tvl_breakdown_filtered_with_min_dt,
             view_daily_op_chains_usage, view_latest_defillama_dexs_protocols_metadata_v1,
             view_stablecoins_metadata_v1_latest_price, view_l2beat_daily_tvl_history], metadata={
    "view_name": "view_daily_chain_usage_data",
    "asset_id": "view_daily_chain_usage_data",
    "direct_dependencies_count": 35,
    "dependents_count": 0,
    "node_type": "view",
    "asset_type": "view"
})
def view_daily_chain_usage_data(context: OpExecutionContext):
    """
View: view_daily_chain_usage_data
Type: view
Direct dependencies: 35
Used by: 0 assets
"""
    context.log.info("Processing view: view_daily_chain_usage_data")
    # View implementation would query BigQuery and return results


@asset(deps=[view_daily_chain_usage_data], metadata={
    "table_name": "oplabs-tools-data.materialized_tables.daily_chain_usage_data_mv",
    "group_id": "daily_chain_usage_data_mv_table_dfb697f2",
    "columns": [
        "alignment",
        "all_chain_keys",
        "app_tvl",
        "app_tvl_usd_dex",
        "app_tvl_usd_eth",
        "app_tvl_usd_lrt",
        "app_tvl_usd_lst",
        "app_tvl_usd_rwa",
        "app_tvl_usd_stablecoins",
        "app_tvl_usd_wrapped",
        "avg_base_fee_gwei",
        "avg_blob_base_fee_on_l1_inbox",
        "avg_blob_base_fee_on_l2",
        "avg_block_time_sec",
        "avg_eth_usd_price",
        "avg_l1_blob_fee_scalar",
        "avg_l1_fee_scalar",
        "avg_l1_gas_price_on_l1_fp_resolve",
        "avg_l1_gas_price_on_l1_inbox",
        "avg_l1_gas_price_on_l1_output",
        "avg_l1_gas_price_on_l2",
        "avg_l2_gas_price_gwei",
        "avg_l2_usd_fees_per_tx_l1_blob_fee",
        "avg_l2_usd_fees_per_tx_l1_fp_resolve",
        "avg_l2_usd_fees_per_tx_l1_inbox_l1gas_fee",
        "avg_l2_usd_fees_per_tx_l1_margin",
        "avg_l2_usd_fees_per_tx_l1_output",
        "avg_l2_usd_fees_per_tx_l2_base_fee",
        "avg_l2_usd_fees_per_tx_l2_priority_fee",
        "avg_priority_fee_gwei",
        "avg_secs_between_l1_txs_inbox",
        "avg_secs_between_l1_txs_output",
        "blocks_per_day",
        "calldata_bytes_l1_inbox",
        "calldata_bytes_user_txs_l2_per_day",
        "chain_id",
        "chain_key",
        "count_github_developers",
        "count_parent_protocol_28dfees_gt_1m",
        "da_layer",
        "data_sources",
        "display_name",
        "distinct_parent_protocol",
        "distinct_protocol",
        "dt",
        "estimated_size_user_txs",
        "eth_gas_costs_per_day",
        "eth_gas_profit_per_day",
        "eth_gas_revenue_per_day",
        "eth_gas_revenue_per_day_source",
        "gas_token",
        "gas_used_per_day",
        "gas_used_per_second",
        "gas_used_per_second_source",
        "input_calldata_gas_user_txs_l2_per_day",
        "is_current_chain",
        "is_evm",
        "is_op_governed",
        "is_upcoming",
        "l1_blobgas_eth_fees_inbox",
        "l1_blobgas_purchased_inbox",
        "l1_calldata_eth_fees_inbox",
        "l1_contrib_l2_eth_fees_per_day",
        "l1_contrib_l2_eth_margin_per_day",
        "l1_eth_fees_fp_resolve",
        "l1_eth_fees_output",
        "l1_execution_eth_fees_inbox",
        "l1_gas_used_combined",
        "l1_gas_used_fp_resolve",
        "l1_gas_used_inbox",
        "l1_gas_used_output",
        "l1_overhead_eth_fees_inbox",
        "l2_contrib_l2_eth_fees_base_fee_per_day",
        "l2_contrib_l2_eth_fees_priority_fee_per_day",
        "l2_gas_used_per_tx",
        "l2b_stage",
        "layer",
        "lend_total_borrow_usd",
        "lend_total_supply_usd",
        "lend_wt_avg_apy_base_borrow",
        "lend_wt_avg_apy_base_supply",
        "lend_wt_avg_apy_reward_borrow",
        "lend_wt_avg_apy_reward_supply",
        "market_avg_blob_base_fee_gwei",
        "market_avg_l1_base_fee_gwei",
        "median_l2_eth_fees_per_tx",
        "median_l2_usd_fees_per_tx",
        "net_token_flow_14d",
        "net_token_flow_1d",
        "net_token_flow_28d",
        "net_token_flow_365d",
        "net_token_flow_60d",
        "net_token_flow_7d",
        "net_token_flow_90d",
        "num_l1_submissions",
        "num_l1_txs_combined",
        "num_l1_txs_fp_resolve",
        "num_l1_txs_inbox",
        "num_l1_txs_output",
        "onchain_value_usd",
        "onchain_value_usd_canonical",
        "onchain_value_usd_external",
        "onchain_value_usd_native",
        "output_root_layer",
        "provider",
        "provider_entity",
        "provider_entity_w_superchain",
        "qualified_txs_per_day",
        "stables_tvl_usd",
        "success_txs_per_day",
        "total_app_fees_eth_excl_stable_mev",
        "total_app_fees_usd",
        "total_app_fees_usd_excl_stable",
        "total_app_fees_usd_excl_stable_mev",
        "total_app_revenue_usd",
        "total_app_revenue_usd_excl_stable",
        "total_app_revenue_usd_excl_stable_mev",
        "total_chain_fees_usd",
        "total_chain_fees_usd_source",
        "total_chain_revenue_usd",
        "total_dex_volume_usd",
        "total_mev_fees_eth",
        "total_mev_fees_usd",
        "total_mev_revenue_usd",
        "total_rev_fees_eth",
        "total_rev_fees_usd",
        "total_rev_revenue_usd",
        "total_stablecoin_fees_eth",
        "total_stablecoin_fees_usd",
        "total_stablecoin_revenue_usd",
        "txs_per_day",
        "txs_per_day_source",
        "usd_gas_costs_per_day",
        "usd_gas_profit_per_day",
        "usd_gas_revenue_per_day"
    ],
    "usage_count": 0,
    "asset_type": "materialized_table",
    "source_view": "oplabs-tools-data.views.daily_chain_usage_data"
})
def daily_chain_usage_data_mv_table_dfb697f2(context: OpExecutionContext):
    """Materialized table from view: oplabs-tools-data.views.daily_chain_usage_data\nTable: oplabs-tools-data.materialized_tables.daily_chain_usage_data_mv"""
    context.log.info("Processing base table: oplabs-tools-data.materialized_tables.daily_chain_usage_data_mv")
    # Base table asset implementation would go here


@asset(deps=[view_weekly_github_author_counts], metadata={
    "materialized_table": "oplabs-tools-data.materialized_tables.weekly_github_author_counts_mv",
    "source_view": "oplabs-tools-data.views.weekly_github_author_counts",
    "schedule": "unknown",
    "asset_type": "materialized_view"
})
def weekly_github_author_counts_mv_table_8bad5631(context: OpExecutionContext):
    """
Materialized snapshot of view: oplabs-tools-data.views.weekly_github_author_counts
Schedule: unknown
Target table: oplabs-tools-data.materialized_tables.weekly_github_author_counts_mv
"""
    context.log.info(
        "Creating materialized snapshot: oplabs-tools-data.materialized_tables.weekly_github_author_counts_mv")
    # Materialized snapshot implementation would create/update the table


@asset(deps=[view_daily_opcollective_revshare], metadata={
    "materialized_table": "oplabs-tools-data.materialized_tables.daily_opcollective_revshare_mv",
    "source_view": "oplabs-tools-data.views.daily_opcollective_revshare",
    "schedule": "unknown",
    "asset_type": "materialized_view"
})
def daily_opcollective_revshare_mv_table_1488e45c(context: OpExecutionContext):
    """
Materialized snapshot of view: oplabs-tools-data.views.daily_opcollective_revshare
Schedule: unknown
Target table: oplabs-tools-data.materialized_tables.daily_opcollective_revshare_mv
"""
    context.log.info(
        "Creating materialized snapshot: oplabs-tools-data.materialized_tables.daily_opcollective_revshare_mv")
    # Materialized snapshot implementation would create/update the table


@asset(deps=[daily_opcollective_revshare_mv_table_1488e45c, materialized_tables_table_88ea7b2b,
             daily_chain_usage_data_mv_table_dfb697f2], metadata={
    "view_name": "view_daily_superchain_health",
    "asset_id": "view_daily_superchain_health",
    "direct_dependencies_count": 3,
    "dependents_count": 0,
    "node_type": "view",
    "asset_type": "view"
})
def view_daily_superchain_health(context: OpExecutionContext):
    """
View: view_daily_superchain_health
Type: view
Direct dependencies: 3
Used by: 0 assets
"""
    context.log.info("Processing view: view_daily_superchain_health")
    # View implementation would query BigQuery and return results


@asset(
    metadata={
        "table_name": "oplabs-tools-data.views.superchain_health_date_ranges",
        "group_id": "superchain_health_date_ranges_table_1568b515",
        "columns": [
            "og_period_end_date",
            "period_end_date",
            "period_start_date",
            "share_elapsed",
            "truncation_type",
        ],
        "usage_count": 0,
        "asset_type": "base_table",
    }
)
def superchain_health_date_ranges_table_1568b515(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.views.superchain_health_date_ranges\nColumns: 5"""
    context.log.info("Processing base table: oplabs-tools-data.views.superchain_health_date_ranges")
    # Base table asset implementation would go here


@asset(
    metadata={
        "table_name": "oplabs-tools-data.temp.gdrive_interop_aop_interopset_daily",
        "group_id": "gdrive_interop_aop_interopset_daily_table_366c40ee",
        "columns": [
            "chain",
            "display_name",
            "dt",
            "is_btc",
            "is_stablecoin",
            "provider_entity_w_superchain",
            "token_type",
            "usd_value",
        ],
        "usage_count": 0,
        "asset_type": "base_table",
    }
)
def gdrive_interop_aop_interopset_daily_table_366c40ee(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.temp.gdrive_interop_aop_interopset_daily\nColumns: 8"""
    context.log.info(
        "Processing base table: oplabs-tools-data.temp.gdrive_interop_aop_interopset_daily"
    )
    # Base table asset implementation would go here


@asset(
    metadata={
        "table_name": "oplabs-tools-data.temp.gdrive_interop_tvl_t1chains_daily",
        "group_id": "gdrive_interop_tvl_t1chains_daily_table_2a24cf20",
        "columns": [
            "app_token_tvl_usd",
            "chain",
            "chain_key",
            "dt",
            "is_btc",
            "is_stablecoin",
            "provider_entity_w_superchain",
            "token_type",
        ],
        "usage_count": 0,
        "asset_type": "base_table",
    }
)
def gdrive_interop_tvl_t1chains_daily_table_2a24cf20(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.temp.gdrive_interop_tvl_t1chains_daily\nColumns: 8"""
    context.log.info(
        "Processing base table: oplabs-tools-data.temp.gdrive_interop_tvl_t1chains_daily"
    )
    # Base table asset implementation would go here


@asset(
    metadata={
        "table_name": "oplabs-tools-data.views.daily_interop_tvl_aop",
        "group_id": "daily_interop_tvl_aop_table_18df9f6c",
        "columns": [
            "aop_btc_interop_usd_value",
            "aop_btc_other_usd_value",
            "aop_eth_usd_value",
            "aop_interop_usd_value",
            "aop_stablecoins_interop_usd_value",
            "aop_stablecoins_other_usd_value",
            "aop_supercoin_interop_usd_value",
            "aop_usd_value",
            "display_name",
            "dt",
            "tvl_btc_interop_usd_value",
            "tvl_btc_other_usd_value",
            "tvl_eth_usd_value",
            "tvl_interop_usd_value",
            "tvl_stablecoins_interop_usd_value",
            "tvl_stablecoins_other_usd_value",
            "tvl_supercoin_interop_usd_value",
            "tvl_usd_value",
        ],
        "usage_count": 0,
        "asset_type": "base_table",
    }
)
def daily_interop_tvl_aop_table_18df9f6c(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.views.daily_interop_tvl_aop\nColumns: 18"""
    context.log.info("Processing base table: oplabs-tools-data.views.daily_interop_tvl_aop")
    # Base table asset implementation would go here


@asset(
    deps=[view_daily_superchain_health, weekly_github_author_counts_mv_table_8bad5631,
          daily_interop_tvl_aop_table_18df9f6c, defillama_daily_distinct_protocols_mv_table_6cd56d61,
          view_daily_superchain_health],
    metadata={
        "view_name": "view_superchain_health_allpds_sheet_view",
        "asset_id": "view_superchain_health_allpds_sheet_view",
        "direct_dependencies_count": 4,
        "dependents_count": 0,
        "node_type": "view",
        "asset_type": "view"
    }
)
def view_superchain_health_allpds_sheet_view(context: OpExecutionContext):
    """Base table column group: oplabs-tools-data.views.superchain_health_date_ranges\nColumns: 5"""
    context.log.info("Processing view: oplabs-tools-data.views.superchain_health_allpds_sheet_view")
    # View implementation would query BigQuery and return results


# =======================================================================
# Network Summary
# =======================================================================


@asset(deps=[view_daily_superchain_health, weekly_github_author_counts_mv_table_8bad5631,
             defillama_daily_distinct_protocols_mv_table_6cd56d61, daily_opcollective_revshare_mv_table_1488e45c])
def hex(context: OpExecutionContext):
    """Asset that returns a hex string"""
    context.log.info("Processing asset: hex")
    return "0x1234"


@asset(metadata={"network_summary": True})
def network_lineage_summary():
    """Summary of the entire lineage network."""

    network_stats = {
        "total_nodes": 93,
        "node_type_counts": {'base_table': 68, 'materialized_view': 7, 'view': 18},
        "materialization_mappings": 5
    }

    # In a real implementation, you might:
    # - Generate network visualization
    # - Perform network analysis (cycles, critical paths)
    # - Create dependency reports
    # - Monitor network health

    return pl.DataFrame([network_stats])
