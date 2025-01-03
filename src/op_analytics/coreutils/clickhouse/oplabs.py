import logging
from typing import Any

import duckdb
import pyarrow as pa
import stamina

import duckdb.typing

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


def generate_clickhouse_ddl(relation: duckdb.DuckDBPyRelation, table_name: str) -> str:
    """Generate a Clickhouse CREATE TABLE statement from a DuckDB relation schema.

    Args:
        relation: DuckDB relation object
        table_name: Name of the table to create

    Returns:
        str: CREATE TABLE statement for Clickhouse
    """
    # Get schema from relation
    schema = zip(relation.columns, relation.types)

    # Map DuckDB types to Clickhouse types
    type_mapping = {
        "VARCHAR": "String",
        "DOUBLE": "Float64",
        "INTEGER": "Int32",
        "BIGINT": "Int64",
        "DATE": "Date",
        "TIMESTAMP": "DateTime",
        "BOOLEAN": "UInt8",
    }

    # Build column definitions
    columns = []
    for col_name, col_type in schema:
        ch_type = type_mapping[str(col_type)]
        columns.append(f"`{col_name}` {ch_type}")

    # Join columns with commas
    columns_str = ",\n    ".join(columns)

    # Build full CREATE TABLE statement
    ddl = f"""CREATE TABLE IF NOT EXISTS {table_name}
(
    {columns_str}
)
ENGINE = ReplacingMergeTree
"""

    return ddl
