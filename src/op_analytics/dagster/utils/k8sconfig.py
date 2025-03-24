import json

from dataclasses import dataclass, field


@dataclass
class OPK8sConfig:
    cpu_request: str = "500m"
    cpu_limit: str = "1000m"

    mem_request: str = "2Gi"
    mem_limit: str = "4Gi"

    labels: dict[str, str] = field(default_factory=dict)

    def construct(self):
        return new_k8s_config(self)


SMALL_POD = OPK8sConfig(
    mem_request="720Mi",
    mem_limit="2Gi",
)

MEDIUM_POD = OPK8sConfig()


def new_k8s_config(custom_config: OPK8sConfig | None):
    k8s_config = custom_config or MEDIUM_POD

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
