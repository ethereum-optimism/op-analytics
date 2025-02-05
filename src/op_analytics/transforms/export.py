from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.env.vault import env_get
from op_analytics.coreutils.bigquery.load import load_unpartitioned_single_uri


from clickhouse_connect.driver.client import Client


log = structlog.get_logger()


def export_to_bigquery(client: Client, db: str, table: str, select_statement):
    """Export data to bigquery by passing first through GCS."""
    KEY_ID = env_get("GCS_HMAC_ACCESS_KEY")
    SECRET = env_get("GCS_HMAC_SECRET")

    gcs_path = f"oplabs-tools-data-sink/clickhouse-exports/{db}/{table}.parquet"

    # Write the data to GCS.
    statement = f"""
    INSERT INTO FUNCTION 
    s3(
        'https://storage.googleapis.com/{gcs_path}',
        '{KEY_ID}',
        '{SECRET}',
        'parquet'
    )
    {select_statement}

    SETTINGS s3_truncate_on_insert = 1
    """
    result = client.command(statement)

    if result.written_rows == 0:  # type: ignore
        raise Exception("empty result on export to bigquery")

    # Create a lod job to write data to BQ.
    load_unpartitioned_single_uri(
        source_uri=f"gs://{gcs_path}",
        dataset=f"clickhouse_export__{db}",
        table=table,
        clustering_fields=None,
    )
    return result
