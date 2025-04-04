import os
from dataclasses import dataclass, field

from op_analytics.coreutils.clickhouse.ddl import read_ddl
from op_analytics.coreutils.clickhouse.inferschema import parquet_to_subquery
from op_analytics.coreutils.clickhouse.oplabs import run_statememt_oplabs
from op_analytics.coreutils.logger import structlog


log = structlog.get_logger()


DIRECTORY = os.path.dirname(__file__)


@dataclass
class ClickHouseDailyDataset:
    """Represent a task to load data to ClickHouse by dt,chain.


    A ClickHouseDailyDataset is defined by a SQL query that reads data for a given dt,chain
    combination, transforms it and writes the result to ClickHouse.

    The input data can be blockbatch data stored in GCS or data already loaded into ClickHouse
    having `dt` and `chain` columns.
    """

    # Output root path determiens the ClickHouse table name where data will be loaded.
    output_root_path: str

    # This is the list of blockbatch root paths that are inputs to this load task.
    inputs_blockbatch: list[str] = field(default_factory=list)

    # This is the list of ClickHouse root paths that are inputs to this load task.
    inputs_clickhouse: list[str] = field(default_factory=list)

    # This is a list of chains for which non_zero row_count will not be enforced.
    ignore_zero_rows_chains: list[str] | None = None

    # This is a list of chain,dt tuples for which non_zero row_count will not be enforced.
    ignore_zero_rows_chain_dts: list[tuple[str, str]] | None = None

    def __post_init__(self):
        for root_path in self.inputs_blockbatch:
            if not root_path.startswith("blockbatch/"):
                raise ValueError(f"Invalid blockbatch input: {root_path}")

        for root_path in self.inputs_clickhouse:
            if not root_path.startswith("blockbatch_daily/"):
                raise ValueError(f"Invalid clickhouse input: {root_path}")

        if not self.output_root_path.startswith("blockbatch_daily/"):
            raise ValueError(f"Invalid output: {self.output_root_path}")

    @staticmethod
    def sanitize_root_path(root_path: str):
        """Use the first part of the root path as the ClickHouse db and the rest as the table."""

        prefix, suffix = root_path.split("/", maxsplit=1)
        return f"{prefix}.{suffix.replace('/', '__')}"

    @property
    def ddl_path(self):
        _, ddl_path = self.output_root_path.split("/", maxsplit=1)
        return f"ddl/{ddl_path}"

    def output_table_name(self):
        return self.sanitize_root_path(self.output_root_path)

    def read_insert_ddl(self):
        return read_ddl(
            path=os.path.join(DIRECTORY, f"{self.ddl_path}__INSERT.sql"),
        )

    def create_table(self):
        create_ddl = read_ddl(
            path=os.path.join(DIRECTORY, f"{self.ddl_path}__CREATE.sql"),
        )

        create_ddl = create_ddl.replace("_placeholder_", self.output_table_name())

        log.info(f"CREATE TABLE {self.output_table_name()}")
        run_statememt_oplabs(statement=create_ddl)

    def insert_ddl_template(self, dry_run: bool = False):
        select_ddl = self.read_insert_ddl()

        # If needed for debugging we can log out the DDL template
        # log.info(ddl)

        # Replace the input tables in the template with s3() table functions.
        for input_root_path in self.inputs_blockbatch:
            input_table = "gcs__" + self.sanitize_root_path(input_root_path)
            input_path = f"gs://oplabs-tools-data-sink/{input_root_path}/INPUT_PARTITION_PATH"
            input_s3 = parquet_to_subquery(gcs_parquet_path=input_path, dry_run=dry_run)
            select_ddl = select_ddl.replace(input_table, input_s3)

        output_table = self.output_table_name()
        insert_ddl = f"INSERT INTO {output_table}\n" + select_ddl
        return insert_ddl
