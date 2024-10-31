from datetime import date
from unittest.mock import MagicMock

from google.cloud import bigquery

from op_coreutils.logger import structlog

from .write import init_client

log = structlog.get_logger()


def load_from_parquet_uris(
    source_uris: list[str],
    source_uri_prefix: str,
    destination_dataset: str,
    destination_table: str,
    date_partition: date,
    time_partition_field: str,
    clustering_fields: list[str] | None,
):
    client = init_client()

    date_suffix = date_partition.strftime("%Y%m%d")
    destination = f"{destination_dataset}.{destination_table}${date_suffix}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=time_partition_field,
            expiration_ms=None,
            require_partition_filter=True,
        ),
        clustering_fields=clustering_fields,
        # NOTE: In case we need to look into automatic schema evolution.
        # Saving these properties so I don't have to look them up again.
        # schema_update_options=[
        #     bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
        #     bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
        # ],
        hive_partitioning=bigquery.HivePartitioningOptions.from_api_repr(
            {
                "mode": "CUSTOM",
                "sourceUriPrefix": source_uri_prefix,
            }
        ),
    )

    job = client.load_table_from_uri(
        source_uris=source_uris,
        destination=destination,
        job_config=job_config,
    )
    job.result()

    operation_prefix = "DRYRUN " if isinstance(client, MagicMock) else ""
    log.info(f"{operation_prefix}{job_config.write_disposition} to BQ {destination}")
