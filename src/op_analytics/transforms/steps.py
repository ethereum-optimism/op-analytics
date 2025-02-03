import os
from dataclasses import dataclass
from typing import Literal

from op_analytics.coreutils.clickhouse.ddl import read_ddls, ClickHouseDDL
from op_analytics.coreutils.logger import structlog

log = structlog.get_logger()

DIRECTORY = os.path.dirname(__file__)


@dataclass
class Step:
    group_name: str
    index: int
    table_name: str
    ddl: ClickHouseDDL

    @property
    def name(self):
        return self.ddl.basename

    @property
    def sql_statement(self):
        """Interpolate the table name on the DDL _placeholder_.

        This ensures the naming convention for the group db is used and is better
        than manually checking that DDL file names agree with table names.
        """
        return self.ddl.statement.replace(
            "_placeholder_",
            f"transforms_{self.group_name}.{self.table_name}",
        )


def read_steps(
    group_name: str,
    step_name: Literal["update"] | Literal["export"],
) -> list[Step]:
    ddls: list[ClickHouseDDL] = read_ddls(
        directory=os.path.join(DIRECTORY, group_name, step_name),
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
                group_name=group_name,
                index=index_int,
                table_name=table_name,
                ddl=ddl,
            )
        )

    return results
