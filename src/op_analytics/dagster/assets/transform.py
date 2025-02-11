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
def erc20_transfers_transforms(context: AssetExecutionContext):
    """Run erc20 transfers dataset transformations."""

    from op_analytics.transforms.main import execute_dt_transforms

    result = execute_dt_transforms(group_name="erc20_transfers", force_complete=True)
    context.log.info(result)
