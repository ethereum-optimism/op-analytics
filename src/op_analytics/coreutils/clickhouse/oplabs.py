import logging
from typing import Any

import pyarrow as pa
import stamina
import polars as pl

from op_analytics.coreutils.logger import structlog

from .client import init_client, run_query, insert, run_statement

log = structlog.get_logger()

ClickHouseInstance = "OPLABS"


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
) -> pl.DataFrame:
    """Run a query on OPLABS ClickHouse instance and return results as Polars DataFrame."""
    return run_query(
        instance=ClickHouseInstance,
        query=query,
        parameters=parameters,
        settings=settings,
    )


def run_statememt_oplabs(
    statement: str,
    parameters: dict[str, Any] | None = None,
    settings: dict[str, Any] | None = None,
) -> dict[str, str]:
    """Run a statement on OPLABS ClickHouse instance."""
    return run_statement(
        instance=ClickHouseInstance,
        statement=statement,
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
    """Insert data into OPLABS ClickHouse instance."""
    return insert(
        instance=ClickHouseInstance,
        database=database,
        table=table,
        df_arrow=df_arrow,
        log_level=log_level,
    )
