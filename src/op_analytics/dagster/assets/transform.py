from dagster import (
    AssetExecutionContext,
    asset,
)


@asset
def interop_transforms(context: AssetExecutionContext):
    """Run interop dataset transformations."""

    from op_analytics.transforms.main import execute_dt_transforms

    result = execute_dt_transforms(group_name="interop", force_complete=True)
    context.log.info(result)


@asset
def transfer_transforms(context: AssetExecutionContext):
    """Run transfer dataset transformations."""

    from op_analytics.transforms.main import execute_dt_transforms

    result = execute_dt_transforms(group_name="transfer", force_complete=True)
    context.log.info(result)
