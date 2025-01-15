import clickhouse_connect

from op_analytics.coreutils.clickhouse.client import init_client
from op_analytics.coreutils.env.vault import env_get
from op_analytics.coreutils.logger import structlog


log = structlog.get_logger()


def create_gcs_view(
    db_name: str,
    table_name: str,
    partition_selection: str,
    gcs_glob_path: str,
):
    """Create a VIEW over data stored in GCS.

    The "partition_selection" parameters is used to add virtual columns to the view.
    This is important because virtual columns are how we make partition path information
    (for example: "chain=base/dt=2025-01-01/") available to consumers of the view.
    """

    KEY_ID = env_get("GCS_HMAC_ACCESS_KEY")
    SECRET = env_get("GCS_HMAC_SECRET")

    db_statement = f"CREATE DATABASE IF NOT EXISTS {db_name}"

    view_statement = f"""
    CREATE VIEW IF NOT EXISTS {db_name}.{table_name} AS 
    SELECT
        {partition_selection} *
    FROM s3(
            'https://storage.googleapis.com/oplabs-tools-data-sink/{gcs_glob_path}',
            '{KEY_ID}',
            '{SECRET}',
            'parquet'
        )
    SETTINGS use_hive_partitioning = 1
    """

    clickhouse_connect.common.set_setting("autogenerate_session_id", True)
    clt = init_client("OPLABS")
    clickhouse_connect.common.set_setting("autogenerate_session_id", False)
    clt.command(db_statement)
    clt.command(view_statement)

    log.info(f"created clickhouse view: {db_name}.{table_name}")
