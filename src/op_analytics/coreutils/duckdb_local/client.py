import os
from threading import Lock

import duckdb
import pyarrow as pa

from op_analytics.coreutils.env.aware import OPLabsEnvironment, current_environment
from op_analytics.coreutils.path import repo_path
from op_analytics.coreutils.logger import structlog, human_rows

log = structlog.get_logger()


_CLIENT: duckdb.DuckDBPyConnection | None = None


_INIT_LOCK = Lock()


def init_client():
    """Idempotent client initialization.

    This function guarantess only one global instance exists.
    """
    global _CLIENT

    with _INIT_LOCK:
        if _CLIENT is None:
            path = repo_path("ozone/duck.db")
            assert path is not None

            os.makedirs(os.path.dirname(path), exist_ok=True)
            _CLIENT = duckdb.connect(path)

            current_env = current_environment()
            if current_env == OPLabsEnvironment.UNITTEST:
                markers_db = "etl_monitor_dev"
            else:
                markers_db = "etl_monitor"

            create_local_tables(_CLIENT, markers_db)

    if _CLIENT is None:
        raise RuntimeError("DuckDB client was not properly initialized.")
    return _CLIENT


def create_local_tables(client, markers_db):
    # Create the schemas we need.
    client.sql(f"CREATE SCHEMA IF NOT EXISTS {markers_db}")

    # Create the tables we need.
    for database, table in [
        ("etl_monitor", "raw_onchain_ingestion_markers"),
        ("etl_monitor", "intermediate_model_markers"),
        ("etl_monitor", "superchain_raw_bigquery_markers"),
    ]:
        ddl_path = repo_path(f"ddl/duckdb_local/{database}.{table}.sql")
        assert ddl_path is not None
        with open(ddl_path, "r") as fobj:
            query = fobj.read().replace(database, markers_db)
            client.sql(query)


def run_query(query: str, params: object = None):
    """Run query"""
    client = init_client()

    return client.sql(query, params=params)


def insert_arrow(database: str, table: str, df_arrow: pa.Table):
    """Write arrow table to local duckdb database."""
    client = init_client()

    my_table = df_arrow
    client.sql(f"INSERT INTO {database}.{table} SELECT * FROM my_table")
    log.info(f"Inserted [{human_rows(len(my_table))}] to {database}.{table}")
