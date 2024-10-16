from threading import Lock
from typing import Any

import clickhouse_connect
from clickhouse_connect.driver.client import Client
import polars as pl

from op_coreutils.env import env_get
from op_coreutils.logger import structlog

log = structlog.get_logger()

_CLIENT: Client | None = None

_INIT_LOCK = Lock()


def init_client():
    """Idempotent client initialization.

    This function guarantess only one global instance exists.
    """
    global _CLIENT

    with _INIT_LOCK:
        if _CLIENT is None:
            # Server-generated ids (as opoosed to client-generated) are required for running
            # concurrent queries. See https://clickhouse.com/docs/en/integrations/python#managing-clickhouse-session-ids.
            clickhouse_connect.common.set_setting("autogenerate_session_id", False)
            _CLIENT = clickhouse_connect.get_client(
                host=env_get("CLICKHOUSE_GOLDSKY_DBT_HOST"),
                port=int(env_get("CLICKHOUSE_GOLDSKY_DBT_PORT")),
                username=env_get("CLICKHOUSE_GOLDSKY_DBT_USER"),
                password=env_get("CLICKHOUSE_GOLDSKY_DBT_PASSWORD"),
            )
            log.info("Initialized Clickhouse client.")

    if _CLIENT is None:
        raise RuntimeError("Clickhouse client was not properly initialized.")

    return _CLIENT


def run_query(
    query: str,
    parameters: dict[str, Any] | None = None,
    settings: dict[str, Any] | None = None,
):
    """Return arrow table with clickhouse results"""
    client = init_client()

    arrow_result = client.query_arrow(
        query=query, parameters=parameters, settings=settings, use_strings=True
    )
    return pl.from_arrow(arrow_result)


def append_df(database: str, table: str, df: pl.DataFrame):
    """Write polars DF to clickhouse."""
    client = init_client()

    client.insert_arrow(table=table, arrow_table=df.to_arrow(), database=database)
    log.info(f"Inserted {len(df)} rows to clickhouse {database}.{table}")
