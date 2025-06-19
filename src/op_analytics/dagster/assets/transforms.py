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

    # Run all steps except for 6 and 7.
    result = execute_dt_transforms(
        group_name="interop",
        force_complete=True,
        range_spec="m2days",
        steps_to_run=None,
        steps_to_skip=[6, 7],
    )
    context.log.info(result)

    # For step 6 we need a back-dated run. What we do is detect ERC-20 create traces
    # for conracts that have had at least one ERC-20 transfer. If we run at the present
    # date then we might get a first transfer for a token that was created in the past.
    # To cover that we sweep over the last 30 days of create traces.
    result = execute_dt_transforms(
        group_name="interop",
        force_complete=True,
        range_spec="m30days",
        steps_to_run=[6],
        steps_to_skip=None,
    )
    context.log.info(result)

    # Run step 7 at the end. This exports the results of step 6 to GCS.
    result = execute_dt_transforms(
        group_name="interop",
        force_complete=True,
        range_spec="m1days",
        steps_to_run=[7],
        steps_to_skip=None,
    )
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


@asset
def fees(context: AssetExecutionContext):
    """Run fees transformations."""
    result = execute_dt_transforms(group_name="fees", force_complete=True)
    context.log.info(result)


@asset
def systemconfig(context: AssetExecutionContext):
    """Run systemconfig transformations."""
    result = execute_dt_transforms(group_name="systemconfig", force_complete=True)
    context.log.info(result)
