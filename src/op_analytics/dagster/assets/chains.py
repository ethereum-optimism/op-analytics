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
def blockbatch_views():
    """Clickhouse external tables over GCS data:

    - blockbatch_gcs.refined_transactions_fees_v1
    - blockbatch_gcs.refined_traces_fees_v1
    """
    from op_analytics.coreutils.clickhouse.gcsview import create_gcs_view

    MODEL_OUTPUTS = [
        ("contract_creation", "create_traces_v1"),
        ("refined_traces", "refined_transactions_fees_v1"),
        ("refined_traces", "refined_traces_fees_v1"),
    ]

    for model, output in MODEL_OUTPUTS:
        create_gcs_view(
            db_name="blockbatch_gcs",
            table_name=f"{model}__{output}",
            partition_selection="chain, CAST(dt as Date) AS dt, ",
            gcs_glob_path=f"blockbatch/{model}/{output}/chain=*/dt=*/*.parquet",
        )
