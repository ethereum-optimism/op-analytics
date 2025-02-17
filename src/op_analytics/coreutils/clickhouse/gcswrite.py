from op_analytics.coreutils.env.vault import env_get


from clickhouse_connect.driver.client import Client

from .client import init_client


def write_to_gcs(client: Client | None, gcs_path: str, select: str):
    """Writes a ClickHouse query directly to GCS."""
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
