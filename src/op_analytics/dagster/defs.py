from dagster import (
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    load_assets_from_package_name,
)

from .utils.k8sconfig import op_analytics_asset_job

defs = Definitions(
    assets=load_assets_from_package_name("op_analytics.dagster.jobs"),
    schedules=[
        #
        # DefiLlama
        ScheduleDefinition(
            name="defillama",
            job=op_analytics_asset_job(
                name="defillama_job",
                selection="volumes_fees_revenue_to_clickhouse",
            ),
            cron_schedule="0 3 * * *",  # Runs at 3 AM daily
            execution_timezone="UTC",
            default_status=DefaultScheduleStatus.RUNNING,
        ),
        #
        # Github Analytics
        ScheduleDefinition(
            name="github_data",
            job=op_analytics_asset_job(
                name="github_data",
                selection="github_data_to_clickhouse",
            ),
            cron_schedule="0 2 * * *",  # Runs at 2 AM daily
            execution_timezone="UTC",
            default_status=DefaultScheduleStatus.RUNNING,
        ),
    ],
)
