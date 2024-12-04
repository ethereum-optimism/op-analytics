import logging
from threading import Lock
from typing import Any, Literal

import clickhouse_connect
import clickhouse_connect.driver.client
import polars as pl
import pyarrow as pa

from op_analytics.coreutils.env import env_get
from op_analytics.coreutils.logger import human_rows, human_size, structlog

log = structlog.get_logger()

ClickHouseInstance = Literal["GOLDSKY", "OPLABS"]


_CLIENTS: dict[ClickHouseInstance, clickhouse_connect.driver.client.Client] = {}


_INIT_LOCK = Lock()


def connect(instance: ClickHouseInstance):
    log.debug(f"connecting to {instance} Clickhouse client...")
    # Server-generated ids (as opposed to client-generated) are required for running
    # concurrent queries. See https://clickhouse.com/docs/en/integrations/python#managing-clickhouse-session-ids.
    clickhouse_connect.common.set_setting("autogenerate_session_id", False)
    client = clickhouse_connect.get_client(
        host=env_get(f"CLICKHOUSE_{instance}_HOST"),
        port=int(env_get(f"CLICKHOUSE_{instance}_PORT")),
        username=env_get(f"CLICKHOUSE_{instance}_USER"),
        password=env_get(f"CLICKHOUSE_{instance}_PASSWORD"),
        connect_timeout=60,
        send_receive_timeout=300,
    )
    log.debug(f"initialized {instance} Clickhouse client.")
    return client


def init_client(instance: ClickHouseInstance, reconnect=False):
    """Idempotent client initialization.

    This function guarantess only one global instance exists.
    """
    global _CLIENTS

    with _INIT_LOCK:
        if _CLIENTS.get(instance) is None or reconnect:
            _CLIENTS[instance] = connect(instance=instance)

    if _CLIENTS.get(instance) is None:
        raise RuntimeError(f"{instance} Clickhouse client was not properly initialized.")

    return _CLIENTS[instance]


def run_query(
    instance: ClickHouseInstance,
    query: str,
    parameters: dict[str, Any] | None = None,
    settings: dict[str, Any] | None = None,
) -> pl.DataFrame | pl.Series:
    """Return arrow table with clickhouse results"""
    client = init_client(instance)

    arrow_result = client.query_arrow(
        query=query,
        parameters=parameters,
        settings=settings,
        use_strings=True,
    )
    return pl.from_arrow(arrow_result)


def run_statement(instance: ClickHouseInstance, statement: str):
    """A statement does not return results."""
    client = init_client(instance)

    client.query(statement)


def insert(
    instance: ClickHouseInstance,
    database: str,
    table: str,
    df_arrow: pa.Table,
    log_level=logging.DEBUG,
):
    """Write arrow table to clickhouse."""
    client = init_client(instance)

    result = client.insert_arrow(
        table=table,
        arrow_table=df_arrow,
        database=database,
    )

    log.log(
        log_level,
        f"done inserting [{human_rows(result.written_rows)} {human_size(result.written_bytes())}] to {database}.{table}",
    )
