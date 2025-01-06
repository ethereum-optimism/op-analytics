import logging
from typing import Any

import pyarrow as pa
import stamina


from op_analytics.coreutils.logger import structlog

from .client import init_client, run_query, insert

log = structlog.get_logger()


def reconnecting_retry(exc: Exception) -> bool:
    log.error(f"Encountered exception: {exc}")
    log.warning("Attempting to reconnect OPLABS client.")
    init_client("OPLABS", reconnect=True)
    return True


@stamina.retry(on=reconnecting_retry, attempts=3)
def run_query_oplabs(
    query: str,
    parameters: dict[str, Any] | None = None,
    settings: dict[str, Any] | None = None,
):
    return run_query(
        instance="OPLABS",
        query=query,
        parameters=parameters,
        settings=settings,
    )


@stamina.retry(on=reconnecting_retry, attempts=3)
def insert_oplabs(
    database: str,
    table: str,
    df_arrow: pa.Table,
    log_level=logging.DEBUG,
):
    """Write arrow table to clickhouse."""
    return insert(
        instance="OPLABS",
        database=database,
        table=table,
        df_arrow=df_arrow,
        log_level=log_level,
    )
