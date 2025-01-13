from dagster import (
    DefaultScheduleStatus,
    In,
    Nothing,
    OpExecutionContext,
    ScheduleDefinition,
    in_process_executor,
    job,
    op,
)

from op_analytics.dagster.utils.k8sconfig import new_k8s_config


@op
def volumes_fees_revenue(context: OpExecutionContext):
    from op_analytics.cli.subcommands.pulls.defillama import volume_fees_revenue

    result = volume_fees_revenue.execute_pull()
    context.log.info(result)


@op(ins={"start": In(Nothing)})
def volumes_fees_revenue_to_clickhouse(context: OpExecutionContext):
    from op_analytics.cli.subcommands.pulls.defillama import volume_fees_revenue

    result = volume_fees_revenue.write_to_clickhouse()
    context.log.info(result)


@job(
    executor_def=in_process_executor,
    tags={"dagster-k8s/config": new_k8s_config()},
)
def defillama_job():
    step1 = volumes_fees_revenue()
    volumes_fees_revenue_to_clickhouse(start=step1)


defillama_schedule = ScheduleDefinition(
    job=defillama_job,
    cron_schedule="0 3 * * *",  # Runs at 3 AM daily
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
)
