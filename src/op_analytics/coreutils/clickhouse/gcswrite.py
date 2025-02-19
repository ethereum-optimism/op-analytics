from op_analytics.coreutils.env.vault import env_get


from clickhouse_connect.driver.client import Client

from .client import init_client


def write_to_gcs(gcs_path: str, select: str, client: Client | None = None):
    """Writes a ClickHouse query directly to GCS.

    See here for documentation: https://clickhouse.com/docs/en/integrations/s3#exporting-data
    """
    client = client or init_client("OPLABS")
    KEY_ID = env_get("GCS_HMAC_ACCESS_KEY")
    SECRET = env_get("GCS_HMAC_SECRET")

    # Write the data to GCS.
    statement = f"""
    INSERT INTO FUNCTION 
    s3(
        'https://storage.googleapis.com/{gcs_path}',
        '{KEY_ID}',
        '{SECRET}',
        'parquet'
    )
    {select}

    SETTINGS s3_truncate_on_insert = 1
    """
    result = client.command(statement)

    if result.written_rows == 0:  # type: ignore
        raise Exception("empty result on write to gcs")
    return result


def write_to_gcs_partitions(
    gcs_root_path: str,
    parquet_filename: str,
    partitions: list[str],
    select: str,
    client: Client | None = None,
):
    """Write a query to GCS partitioned by the provided partition columns.

    IMPORTANT: The parquet files written to GCS will have an additional "_partpath" column
    containing the full path of the partitions in GCS, e.g. "/chain=op/dt=2025-02-16".

    This is because ClickHouse doesn't support "hive-partitioned" writes, where column values
    are set as part of the  GCS path name. Instead, ClickHouse accepts a single {_partition_id}
    placeholder that we take advantage of to create hive partitions by using "_partpath".

    To avoid having fields both in the parquet files and on the partition path we use the
    EXCEPT() modifier on the query to drop the partition columns, which are included in
    "_partpath".

    The additional "_partpath" column will be ignored silently by BigQuery external tables
    that already exist and were created before any parquet files with "_partpath" started
    showing up in the table. This means it is possible to mix and match regular writes with
    GCS writes from ClickHouse.
    """
    partition_by = construct_partition_by(partitions)
    except_cols = ", ".join(partitions)

    select_with_partition_by = f"""
    PARTITION BY _partpath
    SELECT
        {partition_by} AS _partpath,
        * EXCEPT({except_cols})
    FROM (
        {select}
    )
    """

    return write_to_gcs(
        gcs_path=gcs_root_path + "{_partition_id}/" + parquet_filename,
        select=select_with_partition_by,
        client=client,
    )


def construct_partition_by(partitions: list[str]):
    hive_path = ", ".join(f"'/{col}=', toString({col})" for col in partitions)
    return f"CONCAT({hive_path})"
