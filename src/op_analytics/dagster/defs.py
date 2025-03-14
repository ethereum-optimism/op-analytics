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
    "agora",
    "blockbatch",
    "chainsdaily",
    "chainshourly",
    "defillama",
    "dune",
    "github",
    "growthepie",
    "l2beat",
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
        #
        # Blockbatch Models
        create_schedule_for_selection(
            job_name="blockbatch_models_a",
            selection=AssetSelection.assets(
                ["blockbatch", "update_a"],
            ),
            cron_schedule="22,52 * * * *",
            default_status=DefaultScheduleStatus.RUNNING,
            custom_k8s_config=OPK8sConfig(
                mem_request="6Gi",
                mem_limit="4Gi",
                cpu_request="1",
                cpu_limit="1",
            ),
        ),
        #
        # Blockbatch Models
        create_schedule_for_selection(
            job_name="blockbatch_models_b",
            selection=AssetSelection.assets(
                ["blockbatch", "update_b"],
            ),
            cron_schedule="7,37 * * * *",
            default_status=DefaultScheduleStatus.RUNNING,
            custom_k8s_config=OPK8sConfig(
                mem_request="6Gi",
                mem_limit="4Gi",
                cpu_request="1",
                cpu_limit="1",
            ),
        ),
        #
        # Chain related daily jobs
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
        # Chain related hourly jobs
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
        # Defillama protocols
        create_schedule_for_selection(
            job_name="defillama_protocols",
            selection=AssetSelection.assets(
                ["defillama", "protocol_tvl_flows_filtered"],
            ).upstream(),
            cron_schedule="0 12 * * *",
            default_status=DefaultScheduleStatus.RUNNING,
            custom_k8s_config=OPK8sConfig(
                mem_request="3Gi",
                mem_limit="6Gi",
            ),
            k8s_pod_per_step=False,
            job_tags={"dagster/max_retries": 3, "dagster/retry_strategy": "FROM_FAILURE"},
        ),
        #
        # Defillama other
        create_schedule_for_selection(
            job_name="defillama_other",
            selection=AssetSelection.assets(
                ["defillama", "other"],
            ).upstream(),
            cron_schedule="0 14 * * *",
            default_status=DefaultScheduleStatus.RUNNING,
            custom_k8s_config=SMALL_POD,
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
            group="agora",
            cron_schedule="0 10 * * *",
            default_status=DefaultScheduleStatus.RUNNING,
            custom_k8s_config=SMALL_POD,
        ),
        #
        create_schedule_for_group(
            group="growthepie",
            # GrowThePie is generally a little delayed in providing data for the previous day.
            cron_schedule="0 10 * * *",
            default_status=DefaultScheduleStatus.RUNNING,
            custom_k8s_config=SMALL_POD,
        ),
        #
        create_schedule_for_group(
            group="l2beat",
            cron_schedule="0 8 * * *",
            default_status=DefaultScheduleStatus.RUNNING,
            custom_k8s_config=SMALL_POD,
        ),
        #
        create_schedule_for_selection(
            job_name="transforms_teleportr",
            selection=AssetSelection.assets(
                ["transforms", "teleportr"],
            ),
            cron_schedule="47 4,8,14,20 * * *",
            default_status=DefaultScheduleStatus.RUNNING,
            custom_k8s_config=SMALL_POD,
        ),
        #
        create_schedule_for_selection(
            job_name="transforms_dune",
            selection=AssetSelection.assets(
                ["transforms", "dune"],
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
        #
        create_schedule_for_selection(
            job_name="transforms_fees",
            selection=AssetSelection.assets(
                ["transforms", "fees"],
            ),
            cron_schedule="7 4,8,14,20 * * *",
            default_status=DefaultScheduleStatus.RUNNING,
            custom_k8s_config=SMALL_POD,
        ),
    ],
)
