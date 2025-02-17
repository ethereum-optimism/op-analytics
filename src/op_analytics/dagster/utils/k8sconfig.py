import json

from dataclasses import dataclass, field

from dagster import AssetSelection, in_process_executor, define_asset_job
from dagster_k8s import k8s_job_executor


@dataclass
class OPK8sConfig:
    cpu_request: str = "500m"
    cpu_limit: str = "1000m"

    mem_request: str = "2Gi"
    mem_limit: str = "4Gi"

    labels: dict[str, str] = field(default_factory=dict)

    def construct(self):
        return new_k8s_config(self)


def new_k8s_config(custom_config: OPK8sConfig | None):
    k8s_config = custom_config or OPK8sConfig()

    config = {
        "container_config": {
            "resources": {
                "requests": {"cpu": k8s_config.cpu_request, "memory": k8s_config.mem_request},
                "limits": {"cpu": k8s_config.cpu_limit, "memory": k8s_config.mem_limit},
            },
        },
        "pod_template_spec_metadata": {"labels": k8s_config.labels},
    }

    try:
        json.dumps(config)
    except Exception as ex:
        raise ValueError("ivalid configuration. could not dump to json: {config}") from ex

    return config


def op_analytics_asset_job(
    group: str,
    custom_k8s_config: OPK8sConfig | None,
    k8s_pod_per_step: bool,
):
    selection = AssetSelection.groups(group)
    k8s_config = new_k8s_config(custom_k8s_config)

    if k8s_pod_per_step:
        # NOTE: When using pod-per-step individual assets can override the k8s configuration
        # See: https://docs.dagster.io/guides/deploy/deployment-options/kubernetes/customizing-your-deployment#kubernetes-configuration-on-individual-steps-in-a-run
        configured_k8s_executor = k8s_job_executor.configured({"step_k8s_config": k8s_config})
        return define_asset_job(
            name=f"{group}_job",
            selection=selection,
            executor_def=configured_k8s_executor,
        )
    else:
        return define_asset_job(
            name=f"{group}_job",
            selection=selection,
            executor_def=in_process_executor,
            tags={"dagster-k8s/config": k8s_config},
        )
