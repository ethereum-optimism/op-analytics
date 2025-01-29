import re

import clickhouse_connect
import polars as pl

from op_analytics.coreutils.clickhouse.client import new_client
from op_analytics.coreutils.env.vault import env_get
from op_analytics.coreutils.logger import structlog


log = structlog.get_logger()


def parquet_to_subquery(gcs_parquet_path: str, virtual_columns: str = "") -> str:
    """Construct a Clickhouse SELECT statement to read parquet data from GCS."""
    gcs_path = gcs_parquet_path.replace("gs://", "https://storage.googleapis.com/")

    KEY_ID = env_get("GCS_HMAC_ACCESS_KEY")
    SECRET = env_get("GCS_HMAC_SECRET")

    return f"""
    SELECT {virtual_columns} *,
        FROM s3(
            '{gcs_path}',
            '{KEY_ID}',
            '{SECRET}',
            'parquet'
        )
    """


def infer_schema_from_parquet(gcs_parquet_path: str, dummy_name: str):
    """Use a single parquet path to generate a Clickhouse CREATE TABLE dsl statement.

    The value of "dummy_name" is used for the table name in the generated ddl.
    """

    log.info(f"using gcs path: {gcs_parquet_path}")

    statement = f"""
    CREATE TEMPORARY TABLE new_table AS (
        {parquet_to_subquery(gcs_parquet_path)}
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
        col_type = remove_nullable(col["type"])

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
    return ddl


def remove_nullable(data_type: str) -> str:
    """Remove the nullable flag from a Clickhouse data type."""

    return re.sub(
        pattern=r"Nullable\((.*)\)",  # when the pattern matches
        repl=r"\1",  # replace with the first capture in thematch
        string=data_type,
    )
