import os

from clickhouse_connect.driver.summary import QuerySummary
from op_analytics.coreutils.clickhouse.ddl import read_ddls, ClickHouseDDL
from op_analytics.coreutils.clickhouse.client import new_stateful_client
from op_analytics.coreutils.logger import structlog, bound_contextvars
from clickhouse_connect.driver.exceptions import DatabaseError

log = structlog.get_logger()

DIRECTORY = os.path.dirname(__file__)


def create_tables(group_name: str):
    """Find all the CREATE DDLs for this group and run them."""

    ddls: list[ClickHouseDDL] = read_ddls(
        directory=os.path.join(DIRECTORY, group_name, "create"),
        globstr="*.sql",
    )

    client = new_stateful_client("OPLABS")
    results = {}
    for ddl in ddls:
        with bound_contextvars(ddl=ddl.basename):
            # Remove the .sql suffix from the path.
            table_name = ddl.basename.removesuffix(".sql")

            # Interpolate the table name on the DDL _placeholder_. This ensures
            # the naming convention for the group db is used and is better than
            # manually ensuring that DDL file names agree with table names.
            create_statment = ddl.statement.replace(
                "_placeholder_",
                f"transforms_{group_name}.{table_name}",
            )

            try:
                result: QuerySummary = client.command(cmd=create_statment)
            except DatabaseError as ex:
                log.error("database error", exc_info=ex)
                raise

            assert isinstance(result.summary, dict)
            log.info(f"{ddl.basename} -> {result.written_rows} written rows", **result.summary)
            results[ddl.basename] = result.summary

    return results
