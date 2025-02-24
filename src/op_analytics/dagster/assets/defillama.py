from dagster import (
    AssetExecutionContext,
    asset,
)


@asset
def token_mappings_to_bq(context: AssetExecutionContext):
    """Copy token mappings from Google Sheet to BigQuery."""
    from op_analytics.datasources.defillama import token_mappings

    result = token_mappings.execute()
    context.log.info(result)


@asset
def chain_tvl(context: AssetExecutionContext):
    """Pull historical chain tvl data."""
    from op_analytics.datasources.defillama.chaintvl import execute

    result = execute.execute_pull()
    context.log.info(result)


@asset
def protocol_tvl(context: AssetExecutionContext):
    """Pull historical protocol tvl data."""

    from op_analytics.datasources.defillama.protocolstvl import execute

    result = execute.execute_pull()
    context.log.info(result)


@asset(deps=[protocol_tvl, token_mappings_to_bq])
def protocol_tvl_enrichment(context: AssetExecutionContext):
    """Enrich protocol tvl data."""

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
def stablecoins(context: AssetExecutionContext):
    """Pull stablecoin data."""
    from op_analytics.datasources.defillama import stablecoins

    result = stablecoins.execute_pull()
    context.log.info(result)


@asset
def volumes_fees_revenue(context: AssetExecutionContext):
    """Pull DEX data."""
    from op_analytics.datasources.defillama.volumefeesrevenue import execute

    result = execute.execute_pull()
    context.log.info(result)


@asset
def yield_pools(context: AssetExecutionContext):
    """Pull yield pools data."""
    from op_analytics.datasources.defillama.yieldpools import execute

    result = execute.execute_pull()
    context.log.info(result)


@asset
def lend_borrow_pools(context: AssetExecutionContext):
    """Pull lend borrow pools data."""
    from op_analytics.datasources.defillama import lend_borrow_pools

    result = lend_borrow_pools.execute_pull()
    context.log.info(result)


@asset(
    deps=[
        stablecoins,
        protocol_tvl,
        protocol_tvl_enrichment,
        chain_tvl,
        volumes_fees_revenue,
        yield_pools,
        lend_borrow_pools,
    ]
)
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
    DefiLlama.LEND_BORROW_POOLS_HISTORICAL.create_bigquery_external_table()

    DefiLlama.TOKEN_MAPPINGS.create_bigquery_external_table()
