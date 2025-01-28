import os
import socket
from dataclasses import asdict, dataclass
from datetime import date

import polars as pl

from op_analytics.coreutils.clickhouse.inferschema import parquet_to_subquery
from op_analytics.coreutils.clickhouse.oplabs import insert_oplabs, run_statememt_oplabs
from op_analytics.coreutils.logger import structlog, bound_contextvars
from op_analytics.coreutils.time import date_tostr

from .markers import BLOCKBATCH_MARKERS_DW_TABLE

log = structlog.get_logger()

DIRECTORY = os.path.dirname(__file__)


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
    root_path: str
    chain: str
    dt: date
    min_block: int
    data_path: str

    @property
    def context(self):
        return dict(
            chain=self.chain,
            dt=date_tostr(self.dt),
            data_path=self.data_path,
        )

    @property
    def table_name(self):
        return self.root_path.removeprefix("blockbatch/").replace("/", "__")

    def subquery(self):
        return parquet_to_subquery(
            gcs_parquet_path="gs://oplabs-tools-data-sink/" + self.data_path,
            virtual_columns="chain, dt,",
        )

    def execute(self):
        with bound_contextvars(**self.context):
            insert_result = self.write()
            self.write_marker(insert_result)
            return insert_result

    def write(self) -> InsertResult:
        ddl_path = os.path.join(
            DIRECTORY, f"ddl/{self.root_path.removeprefix("blockbatch/")}__INSERT.sql"
        )

        if not os.path.exists(ddl_path):
            raise Exception(f"DDL file not found: {ddl_path}")

        with open(ddl_path, "r") as f:
            ddl = f.read()

        # If needed for debugging we can log out the DDL
        # log.info(ddl)

        # BE CAREFUL! with_subquery may contain HMAC access info.
        # Do not print or log it when debugging.
        with_subquery = ddl.format(subquery=self.subquery())

        result = run_statememt_oplabs(
            statement=with_subquery,
            settings={"use_hive_partitioning": 1},
        )
        insert_result = InsertResult.from_raw(result)
        log.info("insert results", **insert_result.to_dict())

        if insert_result.read_rows != insert_result.written_rows:
            raise Exception(
                "loading into clickhouse should result in the same number of rows as in GCS."
            )

        return insert_result

    def write_marker(self, insert_result: InsertResult):
        marker_df = pl.DataFrame(
            [
                dict(
                    root_path=self.root_path,
                    chain=self.chain,
                    dt=self.dt,
                    min_block=self.min_block,
                    data_path=self.data_path,
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
