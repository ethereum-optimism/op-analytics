import os
from dataclasses import dataclass
from enum import Enum

from op_analytics.coreutils.clickhouse.ddl import read_ddls, ClickHouseDDL
from op_analytics.coreutils.logger import structlog

from .create import TableStructure

log = structlog.get_logger()

DIRECTORY = os.path.dirname(__file__)


class StepType(str, Enum):
    DIM = "dim"
    FACT = "fact"
    EXPORT = "export"
    AGG = "agg"


@dataclass
class Step:
    index: int
    db: str
    table_name: str
    ddl: ClickHouseDDL
    step_type: StepType

    def __post_init__(self):
        assert self.db.startswith("transforms_")

    @property
    def name(self):
        return self.ddl.basename

    def get_sql_statement(self, table: TableStructure):
        """Interpolate the table name on the DDL _placeholder_.

        This ensures the naming convention for the group db is used and is better
        than manually checking that DDL file names agree with table names.

        If there is no explicit INSERT INTO line in the file we add it.
        """
        # If no INSERT INTO line exists, add it at the start
        if "INSERT INTO" not in self.ddl.statement:
            column_order = ", ".join(_.name for _ in table.columns)

            return f"""
            INSERT INTO {self.db}.{self.table_name} 
            
            SELECT {column_order} FROM (
            
            {self.ddl.statement}
                
            )
            """

        # Replace the _placeholder_ with the file name, which is the
        # name of the table that we are updating.
        return self.ddl.statement.replace(
            "_placeholder_",
            f"{self.db}.{self.table_name}",
        )


def read_steps(group_name: str) -> list[Step]:
    ddls: list[ClickHouseDDL] = read_ddls(
        directory=os.path.join(DIRECTORY, group_name, "update"),
        globstr="*.sql",
    )

    # Remove the .sql suffix and ##_ prefix from the path.
    results = []
    for ddl in ddls:
        update_basename = os.path.basename(ddl.basename)
        index_str = update_basename.split("_")[0]
        index_int = int(index_str)
        table_name = update_basename.removesuffix(".sql").removeprefix(index_str + "_")
        results.append(
            Step(
                index=index_int,
                db=f"transforms_{group_name}",
                table_name=table_name,
                ddl=ddl,
                step_type=StepType(table_name.split("_")[0]),
            )
        )

    return results
