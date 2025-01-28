from dagster import (
    OpExecutionContext,
    asset,
)


@asset
def token_mappings_to_bq(context: OpExecutionContext):
    from op_analytics.datasources.defillama import token_mappings

    result = token_mappings.execute()
    context.log.info(result)


@asset
def stablecoins(context: OpExecutionContext):
    """Pull stablecoin data from Defillama."""
    from op_analytics.datasources.defillama import stablecoins

    result = stablecoins.execute_pull()
    context.log.info(result)


@asset
def protocol_tvl(context: OpExecutionContext):
    """Pull historical chain tvl data from Defillama."""

    from op_analytics.datasources.defillama import protocols

    result = protocols.execute_pull()
    context.log.info(result)


@asset(deps=[protocol_tvl])
def tvl_breakdown_enrichment(context: OpExecutionContext):
    """Enrich defillama tvl breakdown data."""

    from op_analytics.datasources.defillama import tvl_breakdown_enrichment

    result = tvl_breakdown_enrichment.execute_pull()
    context.log.info(result)


@asset
def historical_chain_tvl(context: OpExecutionContext):
    """Pull historical chain tvl data from Defillama."""
    from op_analytics.datasources.defillama import historical_chain_tvl

    result = historical_chain_tvl.execute_pull()
    context.log.info(result)


@asset
def volumes_fees_revenue(context: OpExecutionContext):
    from op_analytics.datasources.defillama.volume_fees_revenue import execute

    result = execute.execute_pull()
    context.log.info(result)


@asset
def yield_data(context: OpExecutionContext):
    from op_analytics.datasources.defillama import yield_pools

    result = yield_pools.execute_pull()
    context.log.info(result)


@asset(deps=[stablecoins])
def stablecoins_views():
    """Clickhouse external tables over GCS data:

    - defillama_gcs.stablecoins_metadata_v1
    - defillama_gcs.stablecoins_balances_v1
    """
    from op_analytics.datasources.defillama.dataaccess import DefiLlama

    DefiLlama.STABLECOINS_METADATA.create_clickhouse_view()
    DefiLlama.STABLECOINS_BALANCE.create_clickhouse_view()


@asset(deps=[protocol_tvl, historical_chain_tvl])
def tvl_views():
    """Clickhouse external tables over GCS data:

    - defillama_gcs.chains_metadata_v1
    - defillama_gcs.historical_chain_tvl_v1
    - defillama_gcs.protocols_metadata_v1
    - defillama_gcs.protocols_tvl_v1
    - defillama_gcs.protocols_token_tvl_v1
    """
    from op_analytics.datasources.defillama.dataaccess import DefiLlama

    DefiLlama.CHAINS_METADATA.create_clickhouse_view()
    DefiLlama.HISTORICAL_CHAIN_TVL.create_clickhouse_view()

    DefiLlama.PROTOCOLS_METADATA.create_clickhouse_view()
    DefiLlama.PROTOCOLS_TVL.create_clickhouse_view()
    DefiLlama.PROTOCOLS_TOKEN_TVL.create_clickhouse_view()


@asset(deps=[volumes_fees_revenue])
def volumes_fees_revenue_views():
    """Clickhouse external tables over GCS data:

    - defillama_gcs.volume_fees_revenue_v1
    - defillama_gcs.volume_fees_revenue_breakdown_v1
    - defillama_gcs.volume_protocols_metadata_v1
    - defillama_gcs.fees_protocols_metadata_v1
    - defillama_gcs.revenue_protocols_metadata_v1
    """
    from op_analytics.datasources.defillama.dataaccess import DefiLlama

    DefiLlama.VOLUME_FEES_REVENUE.create_clickhouse_view()
    DefiLlama.VOLUME_FEES_REVENUE_BREAKDOWN.create_clickhouse_view()
    DefiLlama.VOLUME_PROTOCOLS_METADATA.create_clickhouse_view()
    DefiLlama.FEES_PROTOCOLS_METADATA.create_clickhouse_view()
    DefiLlama.REVENUE_PROTOCOLS_METADATA.create_clickhouse_view()


@asset(deps=[tvl_breakdown_enrichment])
def tvl_breakdown_views():
    """Clickhouse external tables over GCS data:

    - defillama_gcs.protocol_token_tvl_breakdown_v1
    """
    from op_analytics.datasources.defillama.dataaccess import DefiLlama

    DefiLlama.PROTOCOL_TOKEN_TVL_BREAKDOWN.create_clickhouse_view()

    # NEXT STEPS. BigQuery view over the results in GCS
    # DefiLlama.PROTOCOL_TOKEN_TVL_BREAKDOWN.create_bigquery_external_table()

    # CREATE OR REPLACE EXTERNAL TABLE `oplabs-tools-data.temp.defillama_tvl_breakdown_enriched`
    # WITH PARTITION COLUMNS (
    # dt DATE
    # )
    # OPTIONS (
    # format = 'PARQUET',
    # uris = ['gs://oplabs-tools-data-sink/defillama/protocol_token_tvl_breakdown_v1/dt=*'],
    # hive_partition_uri_prefix = 'gs://oplabs-tools-data-sink/defillama/protocol_token_tvl_breakdown_v1/'
    # )

    # NEXT STEPS.
    # Suppose we want to create a BigQuery view that joins a bunch of tables.
    # from op_analytics.coreutils.bigquery.write import init_client
    # bq_client = init_client()
    # bq_client.sql("""
    # CREATE VIEW IF NOT EXISTS my_view AS
    # SELECT * FROM JOIN EVERYTHING ELSE
    # """)


# TODO: Consider not doing this anymore now that we have views over GCS data.
@asset(deps=[volumes_fees_revenue])
def volumes_fees_revenue_to_clickhouse(context: OpExecutionContext):
    from op_analytics.datasources.defillama.volume_fees_revenue import execute

    result = execute.write_to_clickhouse()
    context.log.info(result)
