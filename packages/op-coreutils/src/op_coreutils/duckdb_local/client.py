import os
from threading import Lock

import duckdb
import pyarrow as pa

from op_coreutils.path import repo_path
from op_coreutils.logger import structlog, human_rows

log = structlog.get_logger()


_CLIENT: duckdb.DuckDBPyConnection | None = None


_INIT_LOCK = Lock()


def init_client():
    """Idempotent client initialization.

    This function guarantess only one global instance exists.
    """
    global _CLIENT

    with _INIT_LOCK:
        path = repo_path("ozone/duck.db")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        _CLIENT = duckdb.connect(path)

        # Create the schemas we need.
        _CLIENT.sql("CREATE SCHEMA IF NOT EXISTS etl_monitor")

        # Create the tables we need.
        for database, table in [("etl_monitor", "raw_onchain_ingestion_markers")]:
            with open(repo_path(f"ddl/duckdb_local/{database}.{table}.sql"), "r") as fobj:
                _CLIENT.sql(fobj.read())

    if _CLIENT is None:
        raise RuntimeError("DuckDB client was not properly initialized.")
    return _CLIENT


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
