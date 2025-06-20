from dagster import (
    AssetSelection,
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
    "blockbatchingest",
    "blockbatchprocess",
    "blockbatchload",
    "blockbatchloaddaily",
    "bqpublic",
    "chainsdaily",
    "chain_metadata",
    "defillama",
    "dune",
    "github",
    "governance",
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
        # Blockbatch Ingestion
        create_schedule_for_selection(
            job_name="blockbatch_ingest",
            selection=AssetSelection.assets(
                ["blockbatchingest", "audit_and_ingest"],
            ),
            cron_schedule="8,38 * * * *",
            custom_k8s_config=OPK8sConfig(
                mem_request="6Gi",
                mem_limit="8Gi",
                cpu_request="1",
                cpu_limit="1",
            ),
            num_retries=None,
        ),
        #
        # Blockbatch Models
        create_schedule_for_selection(
            job_name="blockbatch_models_a",
            selection=AssetSelection.assets(
                ["blockbatchprocess", "update_a"],
            ),
            cron_schedule="22,52 * * * *",
            custom_k8s_config=OPK8sConfig(
                mem_request="4Gi",
                mem_limit="6Gi",
                cpu_request="1",
                cpu_limit="1",
            ),
            num_retries=None,
        ),
        #
        # Blockbatch Models
        create_schedule_for_selection(
            job_name="blockbatch_models_b",
            selection=AssetSelection.assets(
                ["blockbatchprocess", "update_b"],
            ),
            cron_schedule="7,37 * * * *",
            custom_k8s_config=OPK8sConfig(
                mem_request="4Gi",
                mem_limit="6Gi",
                cpu_request="1",
                cpu_limit="1",
            ),
            num_retries=None,
        ),
        #
        # Load blockbatch data into ClickHouse
        create_schedule_for_selection(
            job_name="blockbatch_load",
            selection=AssetSelection.groups("blockbatchload"),
            cron_schedule="38 * * * *",  # Run every hour
            custom_k8s_config=SMALL_POD,
        ),
        #
        # Load blockbatch data into ClickHouse Daily
        create_schedule_for_selection(
            job_name="blockbatch_load_daily",
            selection=AssetSelection.groups("blockbatchloaddaily"),
            cron_schedule="47 4,10,16,22 * * *",
            custom_k8s_config=SMALL_POD,
        ),
        #
        # Load superchain_raw to BQ
        create_schedule_for_selection(
            job_name="bqpublic_raw",
            selection=AssetSelection.assets(
                ["bqpublic", "superchain_raw"],
            ),
            cron_schedule="2 4,16 * * *",
            custom_k8s_config=SMALL_POD,
            num_retries=None,
        ),
        #
        # Load superchain_4337 to BQ
        create_schedule_for_selection(
            job_name="bqpublic_4337",
            selection=AssetSelection.assets(
                ["bqpublic", "superchain_4337"],
            ),
            cron_schedule="32 4,16 * * *",
            custom_k8s_config=SMALL_POD,
            num_retries=None,
        ),
        #
        # Chain related daily jobs
        create_schedule_for_group(
            group="chainsdaily",
            cron_schedule="0 3 * * *",  # Runs at 3 AM daily
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
            cron_schedule="0 16 * * *",
            custom_k8s_config=OPK8sConfig(
                mem_request="3Gi",
                mem_limit="4Gi",
            ),
            k8s_pod_per_step=False,
        ),
        #
        # Defillama Pools
        create_schedule_for_selection(
            job_name="defillama_pools",
            selection=AssetSelection.assets(
                ["defillama", "yield_pools"],
                ["defillama", "lend_borrow_pools"],
            ).upstream(),
            cron_schedule="0 15 * * *",
            custom_k8s_config=OPK8sConfig(
                mem_request="3Gi",
                mem_limit="4Gi",
            ),
            k8s_pod_per_step=False,
        ),
        #
        # Defillama Chain TVL
        create_schedule_for_selection(
            job_name="defillama_chaintvl",
            selection=AssetSelection.assets(
                ["defillama", "chain_tvl"],
            ).upstream(),
            cron_schedule="0 13 * * *",
            custom_k8s_config=SMALL_POD,
        ),
        #
        # Defillama Stablecoins
        create_schedule_for_selection(
            job_name="defillama_stables",
            selection=AssetSelection.assets(
                ["defillama", "stablecoins"],
            ).upstream(),
            cron_schedule="30 13 * * *",
            custom_k8s_config=SMALL_POD,
        ),
        #
        # Defillama Volume, Fees, Revenue
        create_schedule_for_selection(
            job_name="defillama_vfr",
            selection=AssetSelection.assets(
                ["defillama", "volumes_fees_revenue"],
            ).upstream(),
            cron_schedule="0 14 * * *",
            custom_k8s_config=SMALL_POD,
        ),
        #
        create_schedule_for_group(
            group="github",
            cron_schedule="0 2 * * *",  # Runs at 2 AM daily
        ),
        #
        create_schedule_for_group(
            group="dune",
            # Runs at 8 AM daily.
            cron_schedule="0 8 * * *",
        ),
        #
        create_schedule_for_group(
            group="governance",
            cron_schedule="0 10 * * *",
            custom_k8s_config=SMALL_POD,
        ),
        #
        create_schedule_for_group(
            group="growthepie",
            # GrowThePie is generally a little delayed in providing data for the previous day.
            cron_schedule="0 10 * * *",
            custom_k8s_config=SMALL_POD,
        ),
        #
        create_schedule_for_group(
            group="l2beat",
            cron_schedule="0 8 * * *",
            custom_k8s_config=SMALL_POD,
        ),
        #
        create_schedule_for_selection(
            job_name="transforms_teleportr",
            selection=AssetSelection.assets(
                ["transforms", "teleportr"],
            ),
            cron_schedule="47 4,8,14,20 * * *",
            custom_k8s_config=SMALL_POD,
        ),
        #
        create_schedule_for_selection(
            job_name="transforms_dune",
            selection=AssetSelection.assets(
                ["transforms", "dune"],
            ),
            cron_schedule="47 4,8,14,20 * * *",
            custom_k8s_config=SMALL_POD,
        ),
        #
        create_schedule_for_selection(
            job_name="transforms_interop",
            selection=AssetSelection.assets(
                ["transforms", "erc20transfers"],
                ["transforms", "interop"],
            ),
            cron_schedule="17 4,16 * * *",
            custom_k8s_config=SMALL_POD,
        ),
        #
        create_schedule_for_selection(
            job_name="transforms_fees",
            selection=AssetSelection.assets(
                ["transforms", "fees"],
            ),
            cron_schedule="7 4,8,14,20 * * *",
            custom_k8s_config=SMALL_POD,
        ),
        #
        create_schedule_for_selection(
            job_name="transforms_systemconfig",
            selection=AssetSelection.assets(
                ["transforms", "systemconfig"],
            ),
            cron_schedule="7 1,13 * * *",
            custom_k8s_config=SMALL_POD,
        ),
    ],
)
