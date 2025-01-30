import json
import os
import socket
from dataclasses import dataclass
from datetime import date

import polars as pl

from clickhouse_connect.driver.summary import QuerySummary
from op_analytics.coreutils.clickhouse.client import new_stateful_client
from op_analytics.coreutils.clickhouse.ddl import read_ddls, ClickHouseDDL
from op_analytics.coreutils.clickhouse.oplabs import insert_oplabs
from op_analytics.coreutils.logger import structlog, bound_contextvars
from op_analytics.coreutils.time import date_tostr

from clickhouse_connect.driver.exceptions import DatabaseError

from .config import DIRECTORY
from .markers import TRANSFORM_MARKERS_TABLE

log = structlog.get_logger()


@dataclass
class TransformTask:
    table_name: str
    dt: date

    @property
    def context(self):
        return dict(dt=date_tostr(self.dt))

    def execute(self):
        with bound_contextvars(**self.context):
            results = self.run_ddls()
            self.write_marker(results)
            return results

    def run_ddls(self) -> dict[str, dict[str, str]]:
        ddls: list[ClickHouseDDL] = read_ddls(
            directory=DIRECTORY,
            globstr=f"ddl/{self.table_name}/*UPDATE*.sql",
        )

        client = new_stateful_client("OPLABS")
        results = {}
        for ddl in ddls:
            log.info(f"running ddl {ddl.relative_path}")

            try:
                result: QuerySummary = client.command(
                    cmd=ddl.statement, parameters={"dtparam": self.dt}
                )
            except DatabaseError as ex:
                log.error("database error", exc_info=ex)
                raise

            assert isinstance(result.summary, dict)
            log.info(f"{ddl.relative_path} -> {result.written_rows} written rows", **result.summary)

            if result.written_rows == 0:
                raise Exception("possible data quality issue 0 rows were written!")

            results[ddl.relative_path] = result.summary

        return results

    def write_marker(self, results: dict):
        marker_df = pl.DataFrame(
            [
                dict(
                    transform=self.table_name,
                    dt=self.dt,
                    metadata=json.dumps(results),
                    process_name=os.environ.get("PROCESS", "default"),
                    writer_name=socket.gethostname(),
                )
            ]
        )
        insert_oplabs(
            database="etl_monitor",
            table=TRANSFORM_MARKERS_TABLE,
            df_arrow=marker_df.to_arrow(),
        )
