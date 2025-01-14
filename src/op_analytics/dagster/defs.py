from dagster import (
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    load_assets_from_package_name,
)

from .utils.k8sconfig import op_analytics_asset_job

ASSET_MODULES = [
    "defillama",
    "github",
]


ASSETS = [
    asset
    for name in ASSET_MODULES
    for asset in load_assets_from_package_name(
        f"op_analytics.dagster.assets.{name}",
        group_name=name,
        key_prefix=name,
    )
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
