from dagster import (
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    load_assets_from_package_name,
)

import op_analytics.dagster.jobs as jobs

from .utils.k8sconfig import op_analytics_asset_job

defs = Definitions(
    assets=load_assets_from_package_name("op_analytics.dagster.jobs"),
    jobs=[
        jobs.defillama.defillama_job,
    ],
    schedules=[
        jobs.defillama.defillama_schedule,
        #
        # Github Analytics
        ScheduleDefinition(
            name="github_analytics",
            job=op_analytics_asset_job(
                name="github_analytics_job",
                selection="github_data_to_clickhouse",
            ),
            cron_schedule="0 2 * * *",  # Runs at 2 AM daily
            execution_timezone="UTC",
            default_status=DefaultScheduleStatus.RUNNING,
        ),
    ],
)
