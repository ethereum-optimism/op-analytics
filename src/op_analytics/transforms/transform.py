import json
import os
import socket
from dataclasses import asdict, dataclass
from datetime import date
from typing import Any

import polars as pl
import stamina
from clickhouse_connect.driver.exceptions import DatabaseError
from clickhouse_connect.driver.summary import QuerySummary

from op_analytics.coreutils.clickhouse.client import new_stateful_client
from op_analytics.coreutils.clickhouse.oplabs import insert_oplabs
from op_analytics.coreutils.logger import bound_contextvars, structlog

from .create import TableStructure
from .export import export_to_bigquery
from .markers import TRANSFORM_MARKERS_TABLE
from .updates import Step, StepType, read_steps

log = structlog.get_logger()


@dataclass
class UpdateResult:
    name: str
    result: dict

    def to_dict(self):
        return asdict(self)


class NoWrittenRows(Exception):
    pass


@dataclass
class TransformTask:
    group_name: str
    dt: date
    tables: dict[str, TableStructure]
    update_only: list[str] | None
    raise_if_empty: bool

    def execute(self):
        # Run the updates.
        results = self.run_updates()

        # Write completion markers.
        self.write_marker(results)

        return results

    def run_updates(self) -> list[dict[str, Any]]:
        """Find the SQL update files for this task and run them."""
        client = new_stateful_client("OPLABS")

        results: list[UpdateResult] = []

        for step in read_steps(group_name=self.group_name):
            with bound_contextvars(ddl=step.name):
                if self.update_only is not None and step.index not in self.update_only:
                    log.info(f"skipping index={step.index} ddl={step.name}")
                    continue

                result = self.run_update(client, step)
                results.append(
                    UpdateResult(
                        name=step.name,
                        result=result.summary,
                    )
                )

                if step.step_type == StepType.EXPORT:
                    export_to_bigquery(
                        client=client,
                        db=step.db,
                        table=step.table_name,
                        select_statement=f"SELECT * FROM {step.db}.{step.table_name} FINAL",
                    )

        return [_.to_dict() for _ in results]

    def run_update(self, client, step: Step):
        log.info(f"running ddl {step.name}")

        # Find the table structure for this update.
        table: TableStructure = self.tables[step.table_name]

        retrier = stamina.RetryingCaller(
            attempts=2,
            wait_initial=3,
        ).on(should_retry)

        try:
            result: QuerySummary = retrier(
                client.command,
                cmd=step.get_sql_statement(table),
                parameters={"dtparam": self.dt},
                settings={"use_hive_partitioning": 1},
            )
        except DatabaseError as ex:
            log.error("database error", exc_info=ex)
            raise

        assert isinstance(result.summary, dict)
        log.info(f"{step.name} -> {result.written_rows} written rows", **result.summary)

        if step.step_type == StepType.EXPORT or self.raise_if_empty:
            if result.written_rows == 0:
                msg = "possible data quality issue 0 rows were written!"
                log.error(msg)
                raise NoWrittenRows(f"{msg} dt={self.dt}")

        return result

    def write_marker(self, results: list[dict]):
        """Write completion markers for the task."""
        marker_df = pl.DataFrame(
            [
                dict(
                    transform=self.group_name,
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


def should_retry(_ex: Exception):
    """Decide if we should retry the database command for a transform step."""

    if isinstance(_ex, DatabaseError):
        code636 = "ClickHouse error code 636"
        if code636 in str(_ex):
            # Code: 636. DB::Exception: The table structure cannot be extracted from a parquet format file,
            # because there are no files with provided path in S3ObjectStorage or all files are empty. You
            # can specify table structure manually: The table structure cannot be extracted from a parquet
            # format file. You can specify the structure manually. (CANNOT_EXTRACT_TABLE_STRUCTURE)
            log.warning(f"retrying for {code636}")
            return True
    return False
