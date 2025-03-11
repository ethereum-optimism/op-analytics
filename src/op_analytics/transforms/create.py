import os
import re
from dataclasses import dataclass

import polars as pl
from clickhouse_connect.driver.exceptions import DatabaseError
from clickhouse_connect.driver.summary import QuerySummary

from op_analytics.coreutils.clickhouse.client import new_stateful_client
from op_analytics.coreutils.clickhouse.ddl import ClickHouseDDL, read_ddls
from op_analytics.coreutils.logger import bound_contextvars, structlog

log = structlog.get_logger()

DIRECTORY = os.path.dirname(__file__)

NUMBER_PREFIX_RE = re.compile(r"^\d+_")


@dataclass
class TableColumn:
    name: str
    data_type: str


@dataclass
class TableStructure:
    name: str
    columns: list[TableColumn]


@dataclass
class CreateStatement:
    db_name: str
    table_name: str
    statement: str

    def get_structure_query(self) -> str:
        return f"""
            SELECT 
                position,
                name AS column_name,
                type AS data_type
            FROM system.columns
            WHERE database = '{self.db_name}' 
            AND table = '{self.table_name}'
            ORDER BY position
            """

    @classmethod
    def of(cls, group_name: str, ddl: ClickHouseDDL) -> "CreateStatement":
        # Remove the .sql suffix from the path.
        table_name = ddl.basename.removesuffix(".sql")

        # Remove the ##_ prefix if present.
        table_name = NUMBER_PREFIX_RE.sub("", table_name)

        db_name = f"transforms_{group_name}"

        # Interpolate the table name on the DDL _placeholder_. This ensures
        # the naming convention for the group db is used and is better than
        # manually ensuring that DDL file names agree with table names.
        create_statment = ddl.statement.replace(
            "_placeholder_",
            f"{db_name}.{table_name}",
        )

        return cls(
            db_name=db_name,
            table_name=table_name,
            statement=create_statment,
        )


def create_tables(group_name: str) -> dict[str, TableStructure]:
    """Find all the CREATE DDLs for this group and run them."""

    ddls: list[ClickHouseDDL] = read_ddls(
        directory=os.path.join(DIRECTORY, group_name, "create"),
        globstr="*.sql",
    )

    client = new_stateful_client("OPLABS")

    # Create database for this group if it does not exist yet.
    client.command(f"CREATE DATABASE IF NOT EXISTS transforms_{group_name}")

    results = {}
    for ddl in ddls:
        with bound_contextvars(ddl=ddl.basename):
            create = CreateStatement.of(
                group_name=group_name,
                ddl=ddl,
            )

            try:
                result: QuerySummary = client.command(cmd=create.statetement)
            except DatabaseError as ex:
                log.error("database error", exc_info=ex)
                raise

            assert isinstance(result.summary, dict)
            log.info(f"CREATE {ddl.basename}")

            df = pl.from_arrow(client.query_arrow(create.get_structure_query()))
            assert isinstance(df, pl.DataFrame)

            columns: list[TableColumn] = []
            for row in df.to_dicts():
                columns.append(
                    TableColumn(
                        name=row["column_name"],
                        data_type=row["data_type"],
                    ),
                )

            results[create.table_name] = TableStructure(
                name=create.table_name,
                columns=columns,
            )

    return results
