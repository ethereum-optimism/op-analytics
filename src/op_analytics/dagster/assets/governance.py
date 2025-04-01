from dagster import OpExecutionContext, asset

from op_analytics.transforms.main import execute_dt_transforms


@asset
def public_bucket(context: OpExecutionContext):
    """Pull Agora data."""
    from op_analytics.datasources.governance import execute

    result = execute.execute_pull()
    context.log.info(result)


@asset(deps=[public_bucket])
def transforms_governance(context: OpExecutionContext):
    """Execute governance transforms.

    The governance transforms process the raw ingested data and enrich it using raw
    onchain data. This includes:

    - Decoding DelegateVotesChanged events.
    - Joining with raw blocks to provide block timestamps.
    """
    result = execute_dt_transforms(
        group_name="governance",
        raise_if_empty=[1, 16],
        force_complete=True,
    )
    context.log.info(result)


@asset
def dao_powerindex(context: OpExecutionContext):
    """Pull DAO Power Index data."""
    from op_analytics.datasources.daopowerindex import execute

    result = execute.execute_pull()
    context.log.info(result)
