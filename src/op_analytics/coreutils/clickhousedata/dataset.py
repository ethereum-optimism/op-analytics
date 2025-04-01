import inspect
import os
from enum import Enum

import polars as pl

from op_analytics.coreutils.clickhouse.oplabs import insert_oplabs, run_statememt_oplabs
from op_analytics.coreutils.logger import human_rows, human_size, structlog

log = structlog.get_logger()


class ClickhouseDataset(str, Enum):
    """Base class for clickhouse datasets.

    The name of the subclass is the name of the clickhouse database (aka schema) and the enum
    values are names of the tables that are part of it.

    See for example: DaoPowerIndex.
    """

    @classmethod
    def all_tables(cls) -> list["ClickhouseDataset"]:
        return list(cls.__members__.values())

    @property
    def db(self):
        return "datasources_" + self.__class__.__name__.lower()

    @property
    def table(self):
        return self.value

    def read_sql(self, subdir: str):
        directory = os.path.dirname(inspect.getfile(self.__class__))
        ddl_path = os.path.join(directory, f"{subdir}/{self.table}.sql")
        with open(ddl_path, "r") as fobj:
            return fobj.read().replace("_placeholder_", f"{self.db}.{self.table}")

    def create_table(self):
        ddl = self.read_sql(subdir="ddl")
        log.info(f"CREATE {self.db}.{self.table}")
        run_statememt_oplabs(ddl)

    def write(self, dataframe: pl.DataFrame):
        self.create_table()
        summary = insert_oplabs(self.db, self.table, dataframe.to_arrow())

        summary_dict = dict(
            written_bytes=human_size(summary.written_bytes()),
            written_rows=human_rows(summary.written_rows),
        )

        log.info(f"INSERT {self.db}.{self.table}", **summary_dict)
