import os
from dataclasses import dataclass
from enum import Enum

from op_analytics.coreutils.clickhouse.ddl import read_ddls, ClickHouseDDL
from op_analytics.coreutils.logger import structlog

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

    @property
    def sql_statement(self):
        """Interpolate the table name on the DDL _placeholder_.

        This ensures the naming convention for the group db is used and is better
        than manually checking that DDL file names agree with table names.

        If there is no explicit INSERT INTO line in the file we add it.
        """
        # If no INSERT INTO line exists, add it at the start
        if "INSERT INTO" not in self.ddl.statement:
            return f"""
            INSERT INTO {self.db}.{self.table_name}
            
            {self.ddl.statement}
            """

        # Replace the _placeholder_ with the file name, which is the
        # name of the table that we are updating.
        return self.ddl.statement.replace(
            "_placeholder_",
            f"{self.db}.{self.table_name}",
        )

    @property
    def select_statement(self):
        """Extract the SELECT portion of the SQL statement.

        This is used for exports where we need just the SELECT query without
        the INSERT INTO portion.
        """
        lines = self.sql_statement.split("\n")

        # Find first line starting with INSERT INTO
        for i, line in enumerate(lines):
            if line.strip().startswith("INSERT INTO"):
                # Return all lines after this one, joined back together
                return "\n".join(lines[i + 1 :]).strip()

        # Raise if no INSERT INTO found
        raise Exception("could not parse SELECT statement")


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
