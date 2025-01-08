import clickhouse_connect
import polars as pl

from op_analytics.coreutils.clickhouse.client import new_client
from op_analytics.coreutils.env.vault import env_get
from op_analytics.coreutils.logger import structlog


log = structlog.get_logger()


def infer_schema_from_parquet(gcs_parquet_path: str, dummy_name: str):
    """Use a single parquet path to generate a Clickhouse CREATE TABLE dsl statement.

    The value of "dummy_name" is used for the table name in the generated ddl.
    """

    gcs_path = gcs_parquet_path.replace("gs://", "https://storage.googleapis.com/")
    log.info(f"using gcs path: {gcs_path}")

    KEY_ID = env_get("GCS_HMAC_ACCESS_KEY")
    SECRET = env_get("GCS_HMAC_SECRET")
    statement = f"""
    CREATE TEMPORARY TABLE new_table AS (
        SELECT *,
        FROM s3(
            '{gcs_path}',
            '{KEY_ID}',
            '{SECRET}',
            'parquet'
        )
    )
    """

    clickhouse_connect.common.set_setting("autogenerate_session_id", True)
    clt = new_client("OPLABS")
    clickhouse_connect.common.set_setting("autogenerate_session_id", False)
    clt.command(statement)

    df: pl.DataFrame = pl.from_arrow(clt.query_arrow("DESCRIBE new_table"))  # type: ignore
    schema = df.select("name", "type").to_dicts()

    # Build column definitions
    columns = []
    for col in schema:
        col_name = col["name"]
        col_type = col["type"]

        columns.append(f"`{col_name}` {col_type}")

    # Join columns with commas
    columns_str = ",\n    ".join(columns)

    # Build full CREATE TABLE statement
    ddl = f"""CREATE TABLE IF NOT EXISTS {dummy_name}
(
    {columns_str}
)
ENGINE = ReplacingMergeTree
"""

    print(ddl)
