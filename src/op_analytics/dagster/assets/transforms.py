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


<<<<<<< HEAD
# TODO: Schedule the governance data pipeline once it has been prototyped.
# @asset
# def governance(context: AssetExecutionContext):
#     """Run governance data pipeline."""
#     from op_analytics.transforms.main import execute_dt_transforms
#     result = execute_dt_transforms(group_name="governance", force_complete=True)
#     context.log.info(result)
=======
@asset
def dune(context: AssetExecutionContext):
    """Run dune transformations."""
    result = execute_dt_transforms(group_name="dune", force_complete=True)
    context.log.info(result)


@asset
def fees(context: AssetExecutionContext):
    """Run fees transformations."""
    result = execute_dt_transforms(group_name="fees", force_complete=True)
    context.log.info(result)
>>>>>>> 3bab307593c242d4e8ec83ae5585fcabafbfaab8
