from dagster import (
    AssetExecutionContext,
    asset,
)


@asset
def token_mappings_to_bq(context: AssetExecutionContext):
    """Copy token mappings from Google Sheet to BigQuery."""
    from op_analytics.datasources.defillama import token_mappings
    from op_analytics.datasources.defillama.dataaccess import DefiLlama

    result = token_mappings.execute()
    context.log.info(result)

    DefiLlama.TOKEN_MAPPINGS.create_bigquery_external_table_at_default_dt()


@asset
def chain_tvl(context: AssetExecutionContext):
    """Pull historical chain tvl data."""
    from op_analytics.datasources.defillama.chaintvl import execute
    from op_analytics.datasources.defillama.dataaccess import DefiLlama

    result = execute.execute_pull()
    context.log.info(result)

    DefiLlama.CHAINS_METADATA.create_bigquery_external_table()
    DefiLlama.CHAINS_METADATA.create_bigquery_external_table_at_latest_dt()
    DefiLlama.HISTORICAL_CHAIN_TVL.create_bigquery_external_table()


@asset
def protocol_tvl(context: AssetExecutionContext):
    """Pull historical protocol tvl data."""

    from op_analytics.datasources.defillama.protocolstvl import execute
    from op_analytics.datasources.defillama.dataaccess import DefiLlama

    result = execute.execute_pull()
    context.log.info(result)

    DefiLlama.PROTOCOLS_METADATA.create_bigquery_external_table()
    DefiLlama.PROTOCOLS_METADATA.create_bigquery_external_table_at_latest_dt()

    DefiLlama.PROTOCOLS_TVL.create_bigquery_external_table()
    DefiLlama.PROTOCOLS_TOKEN_TVL.create_bigquery_external_table()


@asset(deps=[protocol_tvl, token_mappings_to_bq])
def protocol_tvl_enrichment(context: AssetExecutionContext):
    """Enrich protocol tvl data."""

    from op_analytics.datasources.defillama import tvl_breakdown_enrichment
    from op_analytics.datasources.defillama.dataaccess import DefiLlama
    from op_analytics.datapipeline.etl.bigqueryviews.view import create_view

    result = tvl_breakdown_enrichment.execute_pull()
    context.log.info(result)

    DefiLlama.PROTOCOL_TOKEN_TVL_BREAKDOWN.create_bigquery_external_table()

    create_view(
        db_name="dailydata_defillama",
        view_name="defillama_tvl_breakdown_filtered",
        disposition="replace",
    )


@asset(deps=[protocol_tvl_enrichment])
def protocol_tvl_flows_filtered(context: AssetExecutionContext):
    """Pull lend borrow pools data."""
    from op_analytics.datasources.defillama.protocolstvlenrich import execute
    from op_analytics.datasources.defillama.dataaccess import DefiLlama

    result = execute.execute_pull()
    context.log.info(result)

    DefiLlama.PROTOCOL_TVL_FLOWS_FILTERED.create_bigquery_external_table()


@asset
def stablecoins(context: AssetExecutionContext):
    """Pull stablecoin data."""
    from op_analytics.datasources.defillama.stablecoins import execute
    from op_analytics.datasources.defillama.dataaccess import DefiLlama

    result = execute.execute_pull()
    context.log.info(result)

    DefiLlama.STABLECOINS_METADATA.create_bigquery_external_table()
    DefiLlama.STABLECOINS_METADATA.create_bigquery_external_table_at_latest_dt()
    DefiLlama.STABLECOINS_BALANCE.create_bigquery_external_table()


@asset
def volumes_fees_revenue(context: AssetExecutionContext):
    """Pull DEX data."""
    from op_analytics.datasources.defillama.volumefeesrevenue import execute
    from op_analytics.datasources.defillama.dataaccess import DefiLlama

    result = execute.execute_pull()
    context.log.info(result)

    DefiLlama.VOLUME_FEES_REVENUE.create_bigquery_external_table()
    DefiLlama.VOLUME_FEES_REVENUE_BREAKDOWN.create_bigquery_external_table()

    DefiLlama.VOLUME_PROTOCOLS_METADATA.create_bigquery_external_table()
    DefiLlama.VOLUME_PROTOCOLS_METADATA.create_bigquery_external_table_at_latest_dt()

    DefiLlama.FEES_PROTOCOLS_METADATA.create_bigquery_external_table()
    DefiLlama.FEES_PROTOCOLS_METADATA.create_bigquery_external_table_at_latest_dt()

    DefiLlama.REVENUE_PROTOCOLS_METADATA.create_bigquery_external_table()
    DefiLlama.REVENUE_PROTOCOLS_METADATA.create_bigquery_external_table_at_latest_dt()


@asset
def yield_pools(context: AssetExecutionContext):
    """Pull yield pools data."""
    from op_analytics.datasources.defillama.yieldpools import execute
    from op_analytics.datasources.defillama.dataaccess import DefiLlama

    result = execute.execute_pull()
    context.log.info(result)

    DefiLlama.YIELD_POOLS_HISTORICAL.create_bigquery_external_table()


@asset
def lend_borrow_pools(context: AssetExecutionContext):
    """Pull lend borrow pools data."""
    from op_analytics.datasources.defillama.lendborrowpools import execute
    from op_analytics.datasources.defillama.dataaccess import DefiLlama

    result = execute.execute_pull()
    context.log.info(result)

    DefiLlama.LEND_BORROW_POOLS_HISTORICAL.create_bigquery_external_table()
