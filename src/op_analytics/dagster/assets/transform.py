from dagster import (
    OpExecutionContext,
    asset,
)


@asset
def execute_transforms(context: OpExecutionContext):
    """Load selected blockbatch datasets to ClickHouse."""

    from op_analytics.transforms.main import execute_dt_transforms

    result = execute_dt_transforms(overwrite=True)
    context.log.info(result)
