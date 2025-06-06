from clickhouse_connect.driver.client import Client

from op_analytics.coreutils.bigquery.load import load_unpartitioned_single_uri
from op_analytics.coreutils.clickhouse.gcswrite import write_to_gcs
from op_analytics.coreutils.logger import structlog

log = structlog.get_logger()


def export_to_bigquery(client: Client, db: str, table: str, select_statement):
    """Export data to bigquery by passing first through GCS."""

    # Write to GCS.
    gcs_path = f"oplabs-tools-data-sink/clickhouse-exports/{db}/{table}.parquet"
    result = write_to_gcs(
        gcs_path=gcs_path,
        select=select_statement,
        client=client,
    )

    # Create a load job to write data to BQ.
    load_unpartitioned_single_uri(
        source_uri=f"gs://{gcs_path}",
        dataset=f"clickhouse_export__{db}",
        table=table,
        clustering_fields=None,
    )
    return result
