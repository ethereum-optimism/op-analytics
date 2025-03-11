from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    load_assets_from_modules,
)

from .utils.k8sconfig import OPK8sConfig, SMALL_POD
from .utils.jobs import (
    create_schedule_for_group,
    create_schedule_for_selection,
)

import importlib

MODULE_NAMES = [
    "chainsdaily",
    "chainshourly",
    "defillama",
    "dune",
    "github",
    "other",
    "transforms",
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
        create_schedule_for_group(
            group="chainsdaily",
            cron_schedule="0 3 * * *",  # Runs at 3 AM daily
            default_status=DefaultScheduleStatus.RUNNING,
            custom_k8s_config=OPK8sConfig(
                mem_request="720Mi",
                mem_limit="2Gi",
            ),
        ),
        #
        create_schedule_for_group(
            group="chainshourly",
            cron_schedule="38 * * * *",  # Run every hour
            default_status=DefaultScheduleStatus.RUNNING,
            custom_k8s_config=OPK8sConfig(
                mem_request="720Mi",
                mem_limit="2Gi",
            ),
        ),
        #
        create_schedule_for_group(
            group="defillama",
            cron_schedule="0 14 * * *",
            default_status=DefaultScheduleStatus.RUNNING,
            custom_k8s_config=OPK8sConfig(
                mem_request="3Gi",
                mem_limit="6Gi",
            ),
            k8s_pod_per_step=False,
        ),
        #
        create_schedule_for_group(
            group="github",
            cron_schedule="0 2 * * *",  # Runs at 2 AM daily
            default_status=DefaultScheduleStatus.RUNNING,
        ),
        #
        create_schedule_for_group(
            group="dune",
            # Runs at 8 AM daily.
            cron_schedule="0 8 * * *",
            default_status=DefaultScheduleStatus.RUNNING,
        ),
        #
        create_schedule_for_group(
            group="other",
            # Runs at 10 AM daily.
            # GrowThePie is generally a little delayed in providing data for the previous day.
            cron_schedule="0 10 * * *",
            default_status=DefaultScheduleStatus.RUNNING,
        ),
        #
        create_schedule_for_selection(
            job_name="transforms_misc",
            selection=AssetSelection.assets(
                ["transforms", "teleportr"],
            ),
            cron_schedule="47 4,8,14,20 * * *",
            default_status=DefaultScheduleStatus.RUNNING,
            custom_k8s_config=SMALL_POD,
        ),
        #
        create_schedule_for_selection(
            job_name="transforms_interop",
            selection=AssetSelection.assets(
                ["transforms", "erc20transfers"],
                ["transforms", "interop"],
            ),
            cron_schedule="17 4,8,14,20 * * *",
            default_status=DefaultScheduleStatus.RUNNING,
            custom_k8s_config=SMALL_POD,
        ),
    ],
)
