from dagster import (
    OpExecutionContext,
    asset,
)


@asset
def blockbatch_load(context: OpExecutionContext):
    """Load selected blockbatch datasets to Clickhouse."""
    from op_analytics.datapipeline.etl.blockbatchload.clickhouse.main import load_to_clickhouse

    result = load_to_clickhouse()
    context.log.info(result)
