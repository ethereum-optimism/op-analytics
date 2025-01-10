from dagster import DefaultScheduleStatus, ScheduleDefinition, job, op, in_process_executor


@op
def volumes_fees_revenue():
    from op_analytics.cli.subcommands.pulls.defillama.volume_fees_revenue import (
        pull_dataframes,
    )

    return pull_dataframes()


@op
def mirror_to_clickhouse():
    from op_analytics.cli.subcommands.pulls.defillama.dataaccess import DefiLlama

    # Capture summaries and return them to have info in Dagster
    s1 = DefiLlama.DEXS_PROTOCOLS_METADATA.insert_to_clickhouse()
    s2 = DefiLlama.FEES_PROTOCOLS_METADATA.insert_to_clickhouse()
    s3 = DefiLlama.VOLUME_FEES_REVENUE.insert_to_clickhouse(incremental_overlap=3)
    s4 = DefiLlama.VOLUME_FEES_REVENUE_BREAKDOWN.insert_to_clickhouse(incremental_overlap=3)

    return [s1, s2, s3, s4]


defillama_k8s_config = {
    "container_config": {
        "resources": {
            "requests": {"cpu": "500m", "memory": "720Mi"},
            "limits": {"cpu": "1000m", "memory": "2Gi"},
        },
        "env": [
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
                    "volumeAttributes": {"secretProviderClass": "dagster-secret-provider-class"},
                },
            }
        ],
    },
}


@job(
    executor_def=in_process_executor,
    tags={"dagster-k8s/config": defillama_k8s_config},
)
def defillama_volumes_fees_revenue_job():
    volumes_fees_revenue()


defillama_schedule = ScheduleDefinition(
    job=defillama_volumes_fees_revenue_job,
    cron_schedule="0 3 * * *",  # Runs at 3 AM daily
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
)
