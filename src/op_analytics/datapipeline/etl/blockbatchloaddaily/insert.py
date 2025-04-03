import os
import socket
import time
from dataclasses import asdict, dataclass
from typing import Any

import polars as pl
from clickhouse_connect.driver.exceptions import DatabaseError

from op_analytics.coreutils.clickhouse.oplabs import insert_oplabs, run_statememt_oplabs
from op_analytics.coreutils.logger import bound_contextvars, human_interval, human_rows, structlog

from .loadspec import LoadSpec
from .markers import BLOCKBATCH_MARKERS_DW_TABLE

log = structlog.get_logger()


DIRECTORY = os.path.dirname(__file__)


@dataclass
class DtChainBatch:
    """Represent a single (dt,chain) that needs to be loaded into ClickHouse."""

    chain: str
    dt: str
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
    dataset: LoadSpec
    batch: DtChainBatch

    @property
    def context(self):
        return dict(batch=self.batch.partitioned_path)

    def construct_insert(self, dry_run: bool = False):
        insert_ddl_template = self.dataset.insert_ddl_template(dry_run=dry_run)

        # Replace the INPUT_PARTITION_PATH placeholder in the template
        select_ddl = insert_ddl_template.replace(
            "INPUT_PARTITION_PATH",
            self.batch.partitioned_path,
        )

        return select_ddl

    def dry_run(self):
        insert_ddl = self.construct_insert(dry_run=True)
        print(insert_ddl)

    def execute(self) -> dict[str, Any]:
        with bound_contextvars(**self.context):
            insert_result = self.write()
            self.write_marker(insert_result)

            return dict(
                dt=self.batch.dt,
                chain=self.batch.chain,
                table=self.dataset.output_table_name(),
                written_rows=insert_result.written_rows,
            )

    def write(self) -> InsertResult:
        insert_ddl = self.construct_insert()

        start = time.time()
        # BE CAREFUL! At this point ddl may contain HMAC access info.
        # Do not print or log it when debugging.
        try:
            log.info("running insert")
            result = run_statememt_oplabs(
                statement=insert_ddl,
                settings={"use_hive_partitioning": 1},
            )
        except DatabaseError as ex:
            log.error("database error", exc_info=ex)
            raise

        insert_result = InsertResult.from_raw(result)

        ellapsed = human_interval(int(time.time() - start))
        read_human = human_rows(insert_result.read_rows)
        write_human = human_rows(insert_result.written_rows)
        num_filtered = human_rows(insert_result.read_rows - insert_result.written_rows)
        log.info(
            f"{ellapsed} read {read_human} -> write {write_human} ({num_filtered} filtered out)",
            **result,
        )

        if insert_result.written_rows > insert_result.read_rows:
            raise Exception(
                f"chain={self.batch.chain}, dt={self.batch.dt} loading into clickhouse should not result in more rows"
            )

        if insert_result.written_rows == 0:
            if self.batch.chain in (self.dataset.ignore_non_zero_row_count or []):
                log.warning("loading into clickhouse should not result in 0 rows")
            else:
                raise Exception(
                    f"chain={self.batch.chain}, dt={self.batch.dt} loading into clickhouse should not result in 0 rows"
                )

        return insert_result

    def write_marker(self, insert_result: InsertResult):
        marker_df = pl.DataFrame(
            [
                dict(
                    root_path=self.dataset.output_root_path,
                    chain=self.batch.chain,
                    dt=self.batch.dt,
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
