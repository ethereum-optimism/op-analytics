from op_analytics.coreutils.logger import structlog

from .write import init_client

log = structlog.get_logger()


def create_gcs_view(
    db_name: str,
    table_name: str,
    partition_columns: str,
    partition_prefix: str,
):
    """Create a VIEW over data stored in GCS.

    The "partition_selection" parameters is used to add virtual columns to the view.
    This is important because virtual columns are how we make partition path information
    (for example: "chain=base/dt=2025-01-01/") available to consumers of the view.
    """
    client = init_client()

    # Construct the SQL DDL statement
    table_id = f"{db_name}.{table_name}"
    ddl_statement = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS `oplabs-tools-data.{table_id}`
    WITH PARTITION COLUMNS ({partition_columns}) 
    OPTIONS (
        format = 'PARQUET',
        uris = ['gs://oplabs-tools-data-sink/{partition_prefix}/*'],
        hive_partition_uri_prefix = 'gs://oplabs-tools-data-sink/{partition_prefix}',
        require_hive_partition_filter = true,
        decimal_target_types = ["NUMERIC", "BIGNUMERIC"]
    ) 
    """

    # Execute the DDL statement
    query_job = client.query(ddl_statement)
    query_job.result()  # Wait for the job to complete
    log.info(f"created bigquery external table: {table_id}")
