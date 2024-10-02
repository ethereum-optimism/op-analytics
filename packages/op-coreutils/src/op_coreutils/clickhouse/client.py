import concurrent.futures
import os
from typing import Any

import clickhouse_connect
import polars as pl

from op_coreutils.logger import LOGGER

log = LOGGER.get_logger()

_CLIENT = None


def init_client():
    """Idempotent client initialization.

    This function guarantess only one global instance exists.
    """
    global _CLIENT

    # Server-generated ids (as opoosed to client-generated) are required for running
    # concurrent queries. See https://clickhouse.com/docs/en/integrations/python#managing-clickhouse-session-ids.
    clickhouse_connect.common.set_setting("autogenerate_session_id", False)
    _CLIENT = clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_GOLDSKY_DBT_HOST"],
        port=int(os.environ["CLICKHOUSE_GOLDSKY_DBT_PORT"]),
        username=os.environ["CLICKHOUSE_GOLDSKY_DBT_USER"],
        password=os.environ["CLICKHOUSE_GOLDSKY_DBT_PASSWORD"],
    )
    log.info("Initialized Clickhouse client.")


def run_query(
    query: str,
    parameters: dict[str, Any] | None = None,
    settings: dict[str, Any] | None = None,
):
    """Return arrow table with clickhouse results"""
    init_client()

    return pl.from_arrow(
        _CLIENT.query_arrow(query=query, parameters=parameters, settings=settings, use_strings=True)
    )


def run_queries_concurrently(
    queries: list[str],
    parameters: dict[str, Any] | None = None,
    settings: dict[str, Any] | None = None,
    max_workers: int | None = None,
):
    """Concurrent clickhouse queries require separate client instances."""

    max_workers = max_workers or 1
    results = [None] * len(queries)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for i, query in enumerate(queries):
            future = executor.submit(
                run_query, **dict(query=query, parameters=parameters, settings=settings)
            )
            futures[future] = i

        for future in concurrent.futures.as_completed(futures):
            i = futures[future]
            try:
                results[i] = future.result()
            except Exception:
                log.error(f"Failed to execute query {i+1} of {len(queries)}")
                raise
            else:
                log.info(f"Success query {i+1} of {len(queries)}")

    return results
