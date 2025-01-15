from op_analytics.datasources.growthepie import chains_daily_fundamentals


from dagster import (
    OpExecutionContext,
    asset,
)


@asset
def growthepie(context: OpExecutionContext):
    """Pull daily chain summary fundamentals from GrowThePie."""

    result = chains_daily_fundamentals.execute_pull()
    context.log.info(result)


@asset(deps=[growthepie])
def growthepie_views():
    """Clickhouse external tables over GCS data:

    - defillama_gcs.stablecoins_metadata_v1
    - defillama_gcs.stablecoins_balances_v1
    """
    from op_analytics.datasources.growthepie.dataaccess import GrowThePie

    GrowThePie.FUNDAMENTALS_SUMMARY.create_clickhouse_view()
    GrowThePie.CHAIN_METADATA.create_clickhouse_view()


# TODO: Consider not doing this anymore now that we have views over GCS data.
@asset(deps=[growthepie])
def growthepie_to_clickhouse(context: OpExecutionContext):
    from op_analytics.datasources.growthepie.dataaccess import GrowThePie

    summaries = [
        GrowThePie.FUNDAMENTALS_SUMMARY.insert_to_clickhouse(incremental_overlap=1),
        GrowThePie.CHAIN_METADATA.insert_to_clickhouse(incremental_overlap=1),
    ]
    context.log.info(summaries)
