from dagster import (
    AssetExecutionContext,
    asset,
)

from op_analytics.dagster.utils.k8sconfig import OPK8sConfig


@asset
def token_mappings_to_bq(context: AssetExecutionContext):
    from op_analytics.datasources.defillama import token_mappings

    result = token_mappings.execute()
    context.log.info(result)


@asset
def stablecoins(context: AssetExecutionContext):
    """Pull stablecoin data from Defillama."""
    from op_analytics.datasources.defillama import stablecoins

    result = stablecoins.execute_pull()
    context.log.info(result)


@asset
def protocol_tvl(context: AssetExecutionContext):
    """Pull historical chain tvl data from Defillama."""

    from op_analytics.datasources.defillama.protocolstvl import execute

    result = execute.execute_pull()
    context.log.info(result)


@asset(deps=[protocol_tvl])
def tvl_breakdown_enrichment(context: AssetExecutionContext):
    """Enrich defillama tvl breakdown data."""

    from op_analytics.datasources.defillama import tvl_breakdown_enrichment

    result = tvl_breakdown_enrichment.execute_pull()
    context.log.info(result)

    from op_analytics.datapipeline.etl.bigqueryviews.view import create_view

    create_view(
        db_name="dailydata_defillama",
        view_name="defillama_tvl_breakdown_filtered",
        disposition="replace",
    )


@asset
def historical_chain_tvl(context: AssetExecutionContext):
    """Pull historical chain tvl data from Defillama."""
    from op_analytics.datasources.defillama import historical_chain_tvl

    result = historical_chain_tvl.execute_pull()
    context.log.info(result)


@asset
def volumes_fees_revenue(context: AssetExecutionContext):
    from op_analytics.datasources.defillama.volume_fees_revenue import execute

    result = execute.execute_pull()
    context.log.info(result)


@asset(
    op_tags={
        "dagster-k8s/config": OPK8sConfig(
            mem_request="4Gi",
            mem_limit="8Gi",
        ).construct()
    }
)
def yield_pools_data(context: AssetExecutionContext):
    from op_analytics.datasources.defillama.yieldpools import execute

    result = execute.execute_pull()
    context.log.info(result)


@asset
def defillama_views():
    """Bigquery external tables and views.

    External tables for each of the datasets we ingest to GCS.

    Views to simplify queries.
    """
    from op_analytics.datasources.defillama.dataaccess import DefiLlama

    DefiLlama.CHAINS_METADATA.create_bigquery_external_table()
    DefiLlama.HISTORICAL_CHAIN_TVL.create_bigquery_external_table()

    DefiLlama.PROTOCOLS_METADATA.create_bigquery_external_table()
    DefiLlama.PROTOCOLS_TVL.create_bigquery_external_table()
    DefiLlama.PROTOCOLS_TOKEN_TVL.create_bigquery_external_table()
    DefiLlama.PROTOCOL_TOKEN_TVL_BREAKDOWN.create_bigquery_external_table()

    DefiLlama.STABLECOINS_METADATA.create_bigquery_external_table()
    DefiLlama.STABLECOINS_BALANCE.create_bigquery_external_table()

    DefiLlama.VOLUME_FEES_REVENUE.create_bigquery_external_table()
    DefiLlama.VOLUME_FEES_REVENUE_BREAKDOWN.create_bigquery_external_table()
    DefiLlama.VOLUME_PROTOCOLS_METADATA.create_bigquery_external_table()
    DefiLlama.FEES_PROTOCOLS_METADATA.create_bigquery_external_table()
    DefiLlama.REVENUE_PROTOCOLS_METADATA.create_bigquery_external_table()

    DefiLlama.YIELD_POOLS_HISTORICAL.create_bigquery_external_table()

    DefiLlama.TOKEN_MAPPINGS.create_bigquery_external_table()
