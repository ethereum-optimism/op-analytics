import os
from dataclasses import dataclass

from op_analytics.coreutils.clickhouse.ddl import read_ddl
from op_analytics.coreutils.clickhouse.inferschema import parquet_to_subquery
from op_analytics.coreutils.clickhouse.oplabs import run_statememt_oplabs
from op_analytics.coreutils.logger import structlog


log = structlog.get_logger()


DIRECTORY = os.path.dirname(__file__)


@dataclass
class LoadSpec:
    """The input datasets and output dataset associated with a load into ClickHouse task."""

    input_root_paths: list[str]
    output_root_path: str
    enforce_row_count: bool = True
    enforce_non_zero_row_count: bool = False

    @classmethod
    def pass_through(cls, root_path: str, enforce_row_count: bool = True):
        """Special constructor for the case where the input and output are the same."""
        return cls(
            input_root_paths=[root_path],
            output_root_path=root_path,
            enforce_row_count=enforce_row_count,
        )

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
        for input_root_path in self.input_root_paths:
            input_table = "gcs__" + self.sanitize_root_path(input_root_path)
            input_path = f"gs://oplabs-tools-data-sink/{input_root_path}/INPUT_PARTITION_PATH"
            input_s3 = parquet_to_subquery(gcs_parquet_path=input_path, dry_run=dry_run)
            select_ddl = select_ddl.replace(input_table, input_s3)

        output_table = self.output_table_name()
        insert_ddl = f"INSERT INTO {output_table}\n" + select_ddl
        return insert_ddl
