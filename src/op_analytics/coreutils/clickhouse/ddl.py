import glob
import os
from dataclasses import dataclass

from op_analytics.coreutils.clickhouse.inferschema import infer_schema_from_parquet
from op_analytics.coreutils.clickhouse.oplabs import run_query_oplabs, run_statememt_oplabs
from op_analytics.coreutils.logger import structlog


log = structlog.get_logger()

DIRECTORY = os.path.dirname(__file__)


@dataclass
class ClickHouseTable:
    """Convenience class to help with table creation in ClickHouse.

    The CREATE TABLE ddl should be defined in teh provided directory and path.

    This class provides methods to create the table when it doesn't exist yet.
    """

    ddl_directory: str
    ddl_path: str
    table_db: str
    table_name: str

    def create_if_not_exists(self, data_path: str | None = None):
        if not self.exists():
            # Attempt to create the table.
            created = self.create()

            # The table does not exist and could not be created.
            # Use the first data path to infer the schema and propose DDL for the table.
            if not created:
                self.raise_not_exists(data_path=data_path)

    def create(self):
        ddl = read_ddl(
            directory=self.ddl_directory,
            path=self.ddl_path,
        )
        run_statememt_oplabs(statement=ddl)
        return True

    def exists(self):
        df = run_query_oplabs(
            query="SELECT name FROM system.tables WHERE database = {db:String} AND name = {table:String}",
            parameters={"db": self.table_db, "table": self.table_name},
        )
        if len(df) == 0:
            return False
        return True

    def raise_not_exists(self, data_path: str | None = None):
        # If this ClickHouse table is created by loading data from GCS we can provide an example
        # data_path to help us infer the schema if the table does not exist yet. This gives us a
        # head start on writing the CREATE statement for the table.
        if data_path is not None:
            proposed_ddl = infer_schema_from_parquet(
                gcs_parquet_path="gs://oplabs-tools-data-sink/" + data_path,
                dummy_name=self.table_name,
            )
            ddl_msg = f"Proposed DDL (adjust as needed):\n{proposed_ddl}"

        else:
            ddl_msg = ""

        msg = [f"Table {self.table_name} does not exist.\n", ddl_msg]

        raise Exception("\n".join(msg))


def read_ddl(directory: str, path: str):
    """Read a .sql DDL file from disk."""

    ddl_path = os.path.join(directory, f"ddl/{path}")

    if not os.path.exists(ddl_path):
        raise Exception(f"DDL file not found: {ddl_path}")

    with open(ddl_path, "r") as f:
        return f.read()


@dataclass
class ClickHouseDDL:
    """Conveninece class to hold a ddl while remembering where it came from."""

    relative_path: str
    statement: str


def read_ddls(directory: str, globstr: str) -> list[ClickHouseDDL]:
    """Read all the ddls in the specified directory and glob.

    This is used to load a family of DDLs that need to be executed in sequence.
    """
    fullglobstr = os.path.join(directory, globstr)

    glob_results = glob.glob(fullglobstr)

    ddls: list[ClickHouseDDL] = []
    for path in glob_results:
        # Remove the directory from the path. And keep only after the "ddl/" directory.
        relative_path = path.split(directory)[-1].removeprefix("/ddl/")

        ddls.append(
            ClickHouseDDL(
                relative_path=relative_path,
                statement=read_ddl(directory=directory, path=relative_path),
            )
        )

    return sorted(ddls, key=lambda x: x.relative_path)
