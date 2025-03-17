import os

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


def op_analytics_asset_selection_job(
    job_name: str,
    selection: AssetSelection,
    custom_k8s_config: OPK8sConfig | None,
    k8s_pod_per_step: bool,
    job_tags: dict[str, Any] | None = None,
):
    k8s_config = new_k8s_config(custom_k8s_config)

    # Set the job name.
    k8s_config["job_metadata"] = {"name": job_name}

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
        tags = {
            "dagster-k8s/config": k8s_config,
            **(job_tags or {}),
        }

        return define_asset_job(
            name=job_name,
            selection=selection,
            executor_def=in_process_executor,
            tags=tags,
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
        job=op_analytics_asset_selection_job(
            job_name=f"{group}_job",
            selection=AssetSelection.groups(group),
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


def get_logs_url():
    job_name = os.environ.get("DAGSTER_RUN_JOB_NAME")

    return f"https://optimistic.grafana.net/explore?schemaVersion=1&panes=%7B%22dar%22:%7B%22datasource%22:%22grafanacloud-logs%22,%22queries%22:%5B%7B%22refId%22:%22A%22,%22expr%22:%22%7Bcluster%3D%5C%22oplabs-tools-data-primary%5C%22,%20namespace%3D%5C%22dagster%5C%22%7D%20%7C%3D%20%60{job_name}%60%22,%22queryType%22:%22range%22,%22datasource%22:%7B%22type%22:%22loki%22,%22uid%22:%22grafanacloud-logs%22%7D,%22editorMode%22:%22builder%22,%22direction%22:%22backward%22%7D%5D,%22range%22:%7B%22from%22:%22now-3h%22,%22to%22:%22now%22%7D,%22panelsState%22:%7B%22logs%22:%7B%22visualisationType%22:%22logs%22,%22columns%22:%7B%220%22:%22Time%22,%221%22:%22Line%22%7D,%22labelFieldName%22:%22labels%22%7D%7D%7D%7D&orgId=1"
