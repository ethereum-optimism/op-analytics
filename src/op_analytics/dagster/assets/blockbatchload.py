from dagster import (
    OpExecutionContext,
    asset,
)


@asset
def blockbatch_load(context: OpExecutionContext):
    """Load selected blockbatch datasets to Clickhouse."""
    from op_analytics.datapipeline.etl.blockbatchload.clickhouse.main import (
        load_to_clickhouse,
        GCSData,
    )

    # Datasets that we want to ingest from GCS into clickhouse.
    # Add more root paths here as we ingest more datasets.
    datasets = [
        #
        GCSData(
            root_path="blockbatch/contract_creation/create_traces_v1",
            enforce_row_count=True,
        ),
        #
        GCSData(
            root_path="blockbatch/token_transfers/erc20_transfers_v1",
            # We don't enforce row count for this dataset because the INSERT sql
            # includes a filter to include only rows where amount_lossless is not NULL.
            enforce_row_count=False,
        ),
        #
        GCSData(
            root_path="blockbatch/token_transfers/erc721_transfers_v1",
            enforce_row_count=True,
        ),
    ]

    result = load_to_clickhouse(datasets=datasets)
    context.log.info(result)
