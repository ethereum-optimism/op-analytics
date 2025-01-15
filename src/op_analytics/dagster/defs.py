from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    load_assets_from_modules,
)

from .utils.k8sconfig import op_analytics_asset_job, OPK8sConfig

import importlib

MODULE_NAMES = [
    "chains",
    "defillama",
    "github",
]


ASSETS = [
    asset
    for name in MODULE_NAMES
    for asset in load_assets_from_modules(
        modules=[importlib.import_module(f"op_analytics.dagster.assets.{name}")],
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
                selection=AssetSelection.groups("defillama"),
                custom_config=OPK8sConfig(
                    labels={
                        "op-analytics-dagster-group": "defillama",
                    }
                ),
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
                selection=AssetSelection.groups("github"),
                custom_config=OPK8sConfig(
                    labels={
                        "op-analytics-dagster-group": "github",
                    }
                ),
            ),
            cron_schedule="0 2 * * *",  # Runs at 2 AM daily
            execution_timezone="UTC",
            default_status=DefaultScheduleStatus.STOPPED,
        ),
    ],
)
