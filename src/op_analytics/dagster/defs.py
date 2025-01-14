from dagster import (
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    load_assets_from_modules,
)

from .assets import defillama, github
from .utils.k8sconfig import op_analytics_asset_job


ASSETS = [
    asset
    for group in [
        load_assets_from_modules([defillama], group_name="defillama", key_prefix="defillama"),
        load_assets_from_modules([github], group_name="github", key_prefix="github"),
    ]
    for asset in group
]

defs = Definitions(
    assets=ASSETS,
    schedules=[
        #
        # DefiLlama
        ScheduleDefinition(
            name="defillama",
            job=op_analytics_asset_job(
                name="defillama_job",
                selection="*defillama/volumes_fees_revenue_to_clickhouse",
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
                selection="*github/write_to_clickhouse",
            ),
            cron_schedule="0 2 * * *",  # Runs at 2 AM daily
            execution_timezone="UTC",
            default_status=DefaultScheduleStatus.STOPPED,
        ),
    ],
)
