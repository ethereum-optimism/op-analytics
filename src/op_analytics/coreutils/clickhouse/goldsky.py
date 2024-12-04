import logging
from typing import Any

import pyarrow as pa

from op_analytics.coreutils.logger import structlog

from .client import insert, run_query, run_statement

log = structlog.get_logger()


def run_query_goldsky(
    query: str,
    parameters: dict[str, Any] | None = None,
    settings: dict[str, Any] | None = None,
):
    return run_query(
        instance="GOLDSKY",
        query=query,
        parameters=parameters,
        settings=settings,
    )


def run_statement_goldsky(statement: str):
    return run_statement(
        instance="GOLDSKY",
        statement=statement,
    )


def insert_goldsky(
    database: str,
    table: str,
    df_arrow: pa.Table,
    log_level=logging.DEBUG,
):
    """Write arrow table to clickhouse."""
    return insert(
        instance="GOLDSKY",
        database=database,
        table=table,
        df_arrow=df_arrow,
        log_level=log_level,
    )
