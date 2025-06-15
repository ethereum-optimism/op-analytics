from dagster import (
    OpExecutionContext,
    asset,
)


@asset
def chain_metadata(context: OpExecutionContext):
    """Run various chain metadata related updates.

    - Upload chain_metadata_raw.csv to Google Sheets.
    - Update the OP Analytics Chain Metadata [ADMIN MANAGED] google sheet.
    - Update the Across Superchain Bridge Addresses [ADMIN MANAGED] google sheet.

    TODO: Decide if we want to upload to Dune, Clickhouse, BigQuery. or op-analytics-static repo.
    """
    from op_analytics.datapipeline.chains.upload import upload_all

    upload_all()


@asset
def superchain_token_list(context: OpExecutionContext):
    """Pull superchain token list from ethereum optimism."""
    from op_analytics.datasources.chainsmeta.superchain import tokenlist

    result = tokenlist.execute_pull()
    context.log.info(result)


@asset
def superchain_chain_list(context: OpExecutionContext):
    """Pull superchain chain list from ethereum optimism."""
    from op_analytics.datasources.chainsmeta.superchain import chainlist

    result = chainlist.execute_pull()
    context.log.info(result)


@asset
def superchain_address_list(context: OpExecutionContext):
    """Pull superchain address list from ethereum optimism."""
    from op_analytics.datasources.chainsmeta.superchain import addresslist

    result = addresslist.execute_pull()
    context.log.info(result)


@asset
def system_config(context: OpExecutionContext):
    """Pull system config from rpcs."""
    from op_analytics.datasources.chainsmeta.systemconfig import execute

    result = execute.execute_pull()
    context.log.info(result)


@asset
def erc20tokens(context: OpExecutionContext):
    """Pull metadata for ERC-20 tokens."""
    from op_analytics.datasources.chainsmeta.erc20tokens import execute

    result = execute.execute_pull()
    context.log.info(result)


@asset
def blockbatch_views():
    """Clickhouse parameterized views over GCS data.

    Example usage:

    ```
    SELECT * FROM blockbatch_gcs.read_date(
        rootpath='blockbatch/refined_traces/refined_traces_fees_v1',
        chain='op',
        dt='2025-01-14'
    )
    LIMIT 10
    SETTINGS use_hive_partitioning = 1
    ```

    NOTE: The "use_hive_partitioning = 1" is required or else the dt and chain columns
    will not be availble in the result.
    """
    from op_analytics.coreutils.clickhouse.gcsview import create_blockbatch_gcs_view

    create_blockbatch_gcs_view()


@asset
def blockbatch_views_bq():
    """BigQuery external tables over GCS data. These views can be useful for
    ad-hoc queries since BQ scales more than ClickHouse and and can process
    large date ranges. In ClickHouse we are generlly limited to looking only
    at one date at a time.

    Tables included:

    - blockbatch_gcs.create_traces_v1
    - blockbatch_gcs.refined_transactions_fees_v2
    - blockbatch_gcs.refined_traces_fees_v2
    """
    from op_analytics.coreutils.bigquery.gcsexternal import create_gcs_external_table

    MODEL_OUTPUTS = [
        ("contract_creation", "create_traces_v1"),
        ("refined_traces", "refined_transactions_fees_v2"),
        ("refined_traces", "refined_traces_fees_v2"),
        ("token_transfers", "erc20_transfers_v1"),
        ("token_transfers", "erc721_transfers_v1"),
    ]

    for model, output in MODEL_OUTPUTS:
        create_gcs_external_table(
            db_name="gcs_blockbatch",
            table_name=f"{model}__{output}",
            partition_columns="chain STRING, dt DATE",
            partition_prefix=f"blockbatch/{model}/{output}",
        )
