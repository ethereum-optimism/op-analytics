import json

from dataclasses import dataclass, field

from dagster import (
    in_process_executor,
    define_asset_job,
)
from dagster._core.definitions.asset_selection import CoercibleToAssetSelection


@dataclass
class OPK8sConfig:
    cpu_request: str = "500m"
    mem_request: str = "720Mi"
    cpu_limit: str = "1000m"
    mem_limit: str = "2Gi"

    labels: dict[str, str] = field(default_factory=dict)


def new_k8s_config(custom_config: OPK8sConfig):
    return {
        "container_config": {
            "resources": {
                "requests": {"cpu": custom_config.cpu_request, "memory": custom_config.mem_request},
                "limits": {"cpu": custom_config.cpu_limit, "memory": custom_config.mem_limit},
            },
        },
        "pod_template_spec_metadata": {"labels": custom_config.labels},
    }


def op_analytics_asset_job(
    name: str,
    selection: CoercibleToAssetSelection,
    custom_config: OPK8sConfig,
):
    config = new_k8s_config(custom_config)

    try:
        json.dumps(config)
    except Exception as ex:
        raise ValueError("ivalid configuration. could not dump to json: {config}") from ex

    return define_asset_job(
        name=name,
        selection=selection,
        executor_def=in_process_executor,
        tags={"dagster-k8s/config": config},
    )
