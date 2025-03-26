import os
import socket
from dataclasses import asdict, dataclass
from datetime import date
from typing import Any

import polars as pl

from op_analytics.coreutils.clickhouse.ddl import read_ddl
from op_analytics.coreutils.clickhouse.inferschema import parquet_to_subquery
from op_analytics.coreutils.clickhouse.oplabs import insert_oplabs, run_statememt_oplabs
from op_analytics.coreutils.logger import structlog, bound_contextvars, human_rows
from op_analytics.coreutils.time import date_tostr
from clickhouse_connect.driver.exceptions import DatabaseError

from .markers import BLOCKBATCH_MARKERS_DW_TABLE

log = structlog.get_logger()


DIRECTORY = os.path.dirname(__file__)


@dataclass
class GCSData:
    """Represent data in GCS that need to be loaded into ClickHouse."""

    input_root_paths: list[str]
    output_root_path: str
    enforce_row_count: bool = True

    @classmethod
    def pass_through(cls, root_path: str, enforce_row_count: bool = True):
        return cls(
            input_root_paths=[root_path],
            output_root_path=root_path,
            enforce_row_count=enforce_row_count,
        )

    @staticmethod
    def sanitize_root_path(root_path: str):
        return root_path.removeprefix("blockbatch/").replace("/", "__")

    def input_table_names(self):
        return [self.sanitize_root_path(root_path) for root_path in self.input_root_paths]

    def output_table_name(self):
        return "blockbatch." + self.sanitize_root_path(self.output_root_path)

    def read_insert_ddl(self):
        ddl_path = self.output_root_path.removeprefix("blockbatch/")
        return read_ddl(
            path=os.path.join(DIRECTORY, f"ddl/{ddl_path}__INSERT.sql"),
        )

    def create_table(self):
        ddl_path = self.output_root_path.removeprefix("blockbatch/")
        create_ddl = read_ddl(
            path=os.path.join(DIRECTORY, f"ddl/{ddl_path}__CREATE.sql"),
        )

        create_ddl = create_ddl.replace("OUTPUT_TABLE", self.output_table_name())

        log.info(f"CREATE TABLE {self.output_table_name()}")
        run_statememt_oplabs(statement=create_ddl)


@dataclass
class BlockBatch:
    """Represent a blockbatch that needs to be loaded into ClickHouse."""

    chain: str
    dt: date
    min_block: int
    partitioned_path: str


@dataclass
class InsertResult:
    """Example result dictionary as returned by Clickhouse.

    {
        "read_rows": "117",
        "read_bytes": "211333",
        "written_rows": "117",
        "written_bytes": "1950084",
        "total_rows_to_read": "0",
        "result_rows": "117",
        "result_bytes": "1950084",
        "elapsed_ns": "380943225",
        "query_id": "15d3bd3f-588a-45d6-894e-5d5570eeac7c",
    }
    """

    read_rows: int
    written_rows: int
    read_bytes: int
    written_bytes: int
    elapsed_s: float

    @classmethod
    def from_raw(cls, result: dict[str, str]):
        return cls(
            read_rows=int(result["read_rows"]),
            written_rows=int(result["written_rows"]),
            read_bytes=int(result["read_bytes"]),
            written_bytes=int(result["written_bytes"]),
            elapsed_s=round(int(result["elapsed_ns"]) / 1e9, 1),
        )

    def to_dict(self):
        return asdict(self)


@dataclass
class InsertTask:
    dataset: GCSData
    blockbatch: BlockBatch
    enforce_row_count: bool = False

    @property
    def context(self):
        return dict(blockbatch=self.blockbatch.partitioned_path)

    def construct_insert(self, dry_run: bool = False):
        select_ddl = self.dataset.read_insert_ddl()

        # If needed for debugging we can log out the DDL template
        # log.info(ddl)

        # Replace the input tables in the template with s3() table functions.
        for input_root_path in self.dataset.input_root_paths:
            input_table = "gcs__" + self.dataset.sanitize_root_path(input_root_path)
            input_path = (
                f"gs://oplabs-tools-data-sink/{input_root_path}/{self.blockbatch.partitioned_path}"
            )
            input_s3 = parquet_to_subquery(gcs_parquet_path=input_path, dry_run=dry_run)
            select_ddl = select_ddl.replace(input_table, input_s3)

        # Replace the BLOCKBATCH_MIN_BLOCK placeholder in the template
        # with the min block number of the blockbatch
        select_ddl = select_ddl.replace(
            "BLOCKBATCH_MIN_BLOCK",
            str(self.blockbatch.min_block),
        )

        output_table = self.dataset.output_table_name()
        insert_ddl = f"INSERT INTO {output_table}\n" + select_ddl
        return insert_ddl

    def dry_run(self):
        insert_ddl = self.construct_insert(dry_run=True)
        print(insert_ddl)

    def execute(self) -> dict[str, Any]:
        with bound_contextvars(**self.context):
            insert_result = self.write()
            self.write_marker(insert_result)

            return dict(
                dt=date_tostr(self.blockbatch.dt),
                chain=self.blockbatch.chain,
                table=self.dataset.output_table_name(),
                min_block=self.blockbatch.min_block,
                data_path=f"{self.dataset.output_root_path}/{self.blockbatch.partitioned_path}",
                written_rows=insert_result.written_rows,
            )

    def write(self) -> InsertResult:
        insert_ddl = self.construct_insert()

        # BE CAREFUL! At this point ddl may contain HMAC access info.
        # Do not print or log it when debugging.
        try:
            result = run_statememt_oplabs(
                statement=insert_ddl,
                settings={"use_hive_partitioning": 1},
            )
        except DatabaseError as ex:
            log.error("database error", exc_info=ex)
            raise

        insert_result = InsertResult.from_raw(result)

        if insert_result.written_rows > insert_result.read_rows:
            raise Exception("loading into clickhouse should not result in more rows")

        if insert_result.written_rows < insert_result.read_rows:
            if self.enforce_row_count:
                raise Exception("loading into clickhouse should not result in fewer rows")
            else:
                read_human = human_rows(insert_result.read_rows)
                write_human = human_rows(insert_result.written_rows)
                num_filtered = human_rows(insert_result.read_rows - insert_result.written_rows)
                log.warning(
                    f"read {read_human} -> write {write_human} ({num_filtered} filtered out)",
                )

        return insert_result

    def write_marker(self, insert_result: InsertResult):
        marker_df = pl.DataFrame(
            [
                dict(
                    root_path=self.dataset.output_root_path,
                    chain=self.blockbatch.chain,
                    dt=self.blockbatch.dt,
                    min_block=self.blockbatch.min_block,
                    data_path=self.blockbatch.partitioned_path,
                    loaded_row_count=insert_result.written_rows,
                    process_name=os.environ.get("PROCESS", "default"),
                    writer_name=socket.gethostname(),
                )
            ]
        )
        insert_oplabs(
            database="etl_monitor",
            table=BLOCKBATCH_MARKERS_DW_TABLE,
            df_arrow=marker_df.to_arrow(),
        )
