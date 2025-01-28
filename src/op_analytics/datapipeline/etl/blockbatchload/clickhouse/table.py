import os
from dataclasses import dataclass

from op_analytics.coreutils.clickhouse.inferschema import infer_schema_from_parquet
from op_analytics.coreutils.clickhouse.oplabs import run_query_oplabs, run_statememt_oplabs
from op_analytics.coreutils.logger import structlog

log = structlog.get_logger()

DIRECTORY = os.path.dirname(__file__)


@dataclass
class BlockBatchTable:
    root_path: str

    @property
    def table_name(self):
        return self.root_path.removeprefix("blockbatch/").replace("/", "__")

    def create(self):
        ddl_path = os.path.join(
            DIRECTORY, f"ddl/{self.root_path.removeprefix("blockbatch/")}__CREATE.sql"
        )

        if not os.path.exists(ddl_path):
            return False

        with open(ddl_path, "r") as f:
            ddl = f.read()
        run_statememt_oplabs(statement=ddl)
        return True

    def exists(self):
        df = run_query_oplabs(
            query="SELECT name FROM system.tables WHERE database = {db:String} AND name = {table:String}",
            parameters={"db": "blockbatch", "table": self.table_name},
        )
        if len(df) == 0:
            return False
        return True

    def raise_not_exists(self, data_path: str):
        proposed_ddl = infer_schema_from_parquet(
            gcs_parquet_path="gs://oplabs-tools-data-sink/" + data_path,
            dummy_name=self.table_name,
        )

        msg = [
            f"Table {self.table_name} does not exist.\n",
            "Proposed DDL (adjust as needed):",
            f"{proposed_ddl}",
        ]

        raise Exception("\n".join(msg))
