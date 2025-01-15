from dagster import (
    in_process_executor,
    job,
    define_asset_job,
)


def new_k8s_config(
    cpu_request: str = "500m",
    mem_request: str = "720Mi",
    cpu_limit: str = "1000m",
    mem_limit: str = "2Gi",
):
    return {
        "container_config": {
            "resources": {
                "requests": {"cpu": cpu_request, "memory": mem_request},
                "limits": {"cpu": cpu_limit, "memory": mem_limit},
            },
            "env": [
                {"name": "OPLABS_ENV", "value": "prod"},
                {"name": "OPLABS_RUNTIME", "value": "k8s"},
                {"name": "PLAIN_LOGS", "value": "true"},
            ],
            "volumeMounts": [{"mountPath": "/var/secrets", "name": "opanalyticsvault"}],
        },
        "pod_spec_config": {
            "serviceAccountName": "dagster-service-account",
            "volumes": [
                {
                    "name": "opanalyticsvault",
                    "csi": {
                        "driver": "secrets-store-gke.csi.k8s.io",
                        "readOnly": "true",
                        "volumeAttributes": {
                            "secretProviderClass": "dagster-secret-provider-class"
                        },
                    },
                }
            ],
        },
    }


def op_analytics_k8s_job():
    return job(
        executor_def=in_process_executor,
        tags={"dagster-k8s/config": new_k8s_config()},
    )


def op_analytics_asset_job(name: str, selection: str):
    return define_asset_job(
        name=name,
        selection=selection,
        executor_def=in_process_executor,
        tags={"dagster-k8s/config": new_k8s_config()},
    )
