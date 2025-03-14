from typing import Any
from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    ScheduleDefinition,
    define_asset_job,
    in_process_executor,
)
from dagster_k8s import k8s_job_executor

from .k8sconfig import OPK8sConfig, new_k8s_config


def op_analytics_asset_job(
    group: str,
    custom_k8s_config: OPK8sConfig | None,
    k8s_pod_per_step: bool,
    job_tags: dict[str, Any] | None = None,
):
    return op_analytics_asset_selection_job(
        job_name=f"{group}_job",
        selection=AssetSelection.groups(group),
        custom_k8s_config=custom_k8s_config,
        k8s_pod_per_step=k8s_pod_per_step,
        job_tags=job_tags,
    )


def op_analytics_asset_selection_job(
    job_name: str,
    selection: AssetSelection,
    custom_k8s_config: OPK8sConfig | None,
    k8s_pod_per_step: bool,
    job_tags: dict[str, Any] | None = None,
):
    k8s_config = new_k8s_config(custom_k8s_config)

    if k8s_pod_per_step:
        # NOTE: When using pod-per-step individual assets can override the k8s configuration
        # See: https://docs.dagster.io/guides/deploy/deployment-options/kubernetes/customizing-your-deployment#kubernetes-configuration-on-individual-steps-in-a-run
        configured_k8s_executor = k8s_job_executor.configured({"step_k8s_config": k8s_config})
        return define_asset_job(
            name=job_name,
            selection=selection,
            executor_def=configured_k8s_executor,
            tags=job_tags,
        )
    else:
        return define_asset_job(
            name=job_name,
            selection=selection,
            executor_def=in_process_executor,
            tags={"dagster-k8s/config": k8s_config},
        )


def create_schedule_for_group(
    group: str,
    cron_schedule: str,
    default_status: DefaultScheduleStatus,
    custom_k8s_config: OPK8sConfig | None = None,
    k8s_pod_per_step: bool = False,
    job_tags: dict[str, Any] | None = None,
):
    return ScheduleDefinition(
        name=group,
        job=op_analytics_asset_job(
            group=group,
            custom_k8s_config=custom_k8s_config,
            k8s_pod_per_step=k8s_pod_per_step,
            job_tags=job_tags,
        ),
        cron_schedule=cron_schedule,
        execution_timezone="UTC",
        default_status=default_status,
    )


def create_schedule_for_selection(
    job_name: str,
    selection: AssetSelection,
    cron_schedule: str,
    default_status: DefaultScheduleStatus,
    custom_k8s_config: OPK8sConfig | None = None,
    k8s_pod_per_step: bool = False,
    job_tags: dict[str, Any] | None = None,
):
    return ScheduleDefinition(
        name=job_name,
        job=op_analytics_asset_selection_job(
            job_name=job_name,
            selection=selection,
            custom_k8s_config=custom_k8s_config,
            k8s_pod_per_step=k8s_pod_per_step,
            job_tags=job_tags,
        ),
        cron_schedule=cron_schedule,
        execution_timezone="UTC",
        default_status=default_status,
    )


def create_schedule_for_asset(
    job_name: str,
    asset_name: list[str],
    cron_schedule: str,
    default_status: DefaultScheduleStatus,
    custom_k8s_config: OPK8sConfig | None = None,
    k8s_pod_per_step: bool = False,
):
    return create_schedule_for_selection(
        job_name=job_name,
        selection=AssetSelection.assets(asset_name),
        cron_schedule=cron_schedule,
        default_status=default_status,
        custom_k8s_config=custom_k8s_config,
        k8s_pod_per_step=k8s_pod_per_step,
    )
