from dagster import (
    OpExecutionContext,
    asset,
)

from op_analytics.datapipeline.etl.loadbq.main import (
    load_superchain_4337_to_bq,
    load_superchain_raw_to_bq,
)


@asset
def superchain_raw(context: OpExecutionContext):
    result = load_superchain_raw_to_bq(
        range_spec="m10days",
        dryrun=False,
        force_complete=False,
        force_not_ready=False,
    )
    context.log.info(result)


@asset
def superchain_4337(context: OpExecutionContext):
    result = load_superchain_4337_to_bq(
        range_spec="m10days",
        dryrun=False,
        force_complete=False,
        force_not_ready=False,
    )
    context.log.info(result)
