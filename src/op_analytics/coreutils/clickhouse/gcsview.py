from op_analytics.coreutils.clickhouse.client import init_client
from op_analytics.coreutils.env.vault import env_get
from op_analytics.coreutils.logger import structlog


log = structlog.get_logger()


def create_blockbatch_gcs_view():
    """Create a parameterized view for blockbatch models.

    The parameterized view requires the user to specify the model and the
    chain and dt partitions that will be queried.
    """

    db_name = "blockbatch_gcs"

    KEY_ID = env_get("GCS_HMAC_ACCESS_KEY")
    SECRET = env_get("GCS_HMAC_SECRET")

    db_statement = f"CREATE DATABASE IF NOT EXISTS {db_name}"

    view_name = f"{db_name}.read_date"
    view_statement = f"""
    CREATE VIEW IF NOT EXISTS {view_name} AS
    SELECT
        chain, CAST(dt as Date) AS dt,  *
    FROM s3(
            concat(
                'https://storage.googleapis.com/oplabs-tools-data-sink/',
                {{rootpath:String}},
                '/chain=',
                {{chain:String}},
                '/dt=',
                {{dt:String}},
                '/*.parquet'
            ),
            '{KEY_ID}',
            '{SECRET}',
            'parquet'
        )
    SETTINGS use_hive_partitioning = 1
    """

    clt = init_client("OPLABS")
    clt.command(db_statement)
    clt.command(view_statement)

    log.info(f"created clickhouse parameterized view in db={db_name}")


def create_dailydata_gcs_view():
    """Create parameterized views to read ingested DailyData datasets.

    "read_date"

        This parameterized view requires the user to specify the rootpath and the
        dt partition pattern that will be queried.

    "read_latest"

        This parameterized view automatically loads only the latest "dt" partition.
        It looks at now() and now() - 1 day to find the latest "dt" that has been
        ingested.
    """

    db_name = "dailydata_gcs"

    KEY_ID = env_get("GCS_HMAC_ACCESS_KEY")
    SECRET = env_get("GCS_HMAC_SECRET")

    db_statement = f"CREATE DATABASE IF NOT EXISTS {db_name}"

    clt = init_client("OPLABS")
    clt.command(db_statement)

    # READ_DATE: view to read from any "dt" pattern.
    clt.command(f"""
    CREATE VIEW IF NOT EXISTS {db_name}.read_date AS
    SELECT
        CAST(dt as Date) AS dt,  *
    FROM s3(
            concat(
                'https://storage.googleapis.com/oplabs-tools-data-sink/',
                {{rootpath:String}},
                '/dt=',
                {{dt:String}},
                '/*.parquet'
            ),
            '{KEY_ID}',
            '{SECRET}',
            'parquet'
        )
    SETTINGS use_hive_partitioning = 1
    """)

    # READ_LATEST: view to read only the most recent "dt".
    last_two_days = (
        "'{' || now()::Date::String || ',' || date_sub(DAY, 1, now())::Date::String || '}'"
    )
    clt.command(f"""
    CREATE VIEW IF NOT EXISTS {db_name}.read_latest AS
    WITH latest AS (
        SELECT * FROM
        {db_name}.read_date(
            rootpath = {{rootpath:String}},
            dt = {last_two_days}
        )
    )

    SELECT *
    FROM latest WHERE dt IN (SELECT max(dt) FROM latest)
    SETTINGS use_hive_partitioning = 1
    """)

    clt.command("""
    CREATE FUNCTION IF NOT EXISTS latest_dt AS 
    x -> (SELECT max(dt) FROM etl_monitor.daily_data_markers WHERE (root_path = x) LIMIT 1)
    """)

    log.info(f"created clickhouse parameterized view in db={db_name}")

    # READ_DEFAULT: view to read the default dt=2000-01-01 in clickhouse
    clt.command(f"""
    CREATE VIEW IF NOT EXISTS {db_name}.read_default AS
    SELECT * FROM
    {db_name}.read_date(
        rootpath = {{rootpath:String}},
        dt = '2000-01-01'
    )
    """)

    log.info(f"created clickhouse parameterized view in db={db_name}")
