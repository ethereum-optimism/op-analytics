from dataclasses import dataclass

from op_analytics.coreutils.clickhouse.ddl import ClickHouseTable, read_ddl

import os

# This directory
DIRECTORY = os.path.dirname(__file__)


@dataclass
class BlockBatchTable:
    root_path: str

    @property
    def table_name(self):
        return self.root_path.removeprefix("blockbatch/").replace("/", "__")

    def to_table(self):
        return ClickHouseTable(
            ddl_directory=DIRECTORY,
            ddl_path=f"{self.root_path}__CREATE.sql",
            table_db="blockbatch",
            table_name=self.table_name,
        )

    def read_insert_ddl(self):
        ddl_path = self.root_path.removeprefix("blockbatch/")
        return read_ddl(
            path=os.path.join(DIRECTORY, f"ddl/{ddl_path}__INSERT.sql"),
        )
