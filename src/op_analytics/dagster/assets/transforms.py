from dagster import (
    AssetExecutionContext,
    asset,
)

from op_analytics.transforms.main import execute_dt_transforms


@asset
def erc20transfers(context: AssetExecutionContext):
    """Run erc20 transfers dataset transformations."""
    result = execute_dt_transforms(group_name="erc20transfers", force_complete=True)
    context.log.info(result)


@asset
def interop(context: AssetExecutionContext):
    """Run interop dataset transformations."""
    result = execute_dt_transforms(group_name="interop", force_complete=True)
    context.log.info(result)


@asset
def teleportr(context: AssetExecutionContext):
    """Run teleportr events transformations."""
    result = execute_dt_transforms(group_name="teleportr", force_complete=True)
    context.log.info(result)


@asset
def dune(context: AssetExecutionContext):
    """Run dune transformations."""
    result = execute_dt_transforms(group_name="dune", force_complete=True)
    context.log.info(result)
