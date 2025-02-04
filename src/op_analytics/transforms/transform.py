import json
import os
import socket
from dataclasses import asdict, dataclass
from datetime import date

import polars as pl
from clickhouse_connect.driver.exceptions import DatabaseError
from clickhouse_connect.driver.summary import QuerySummary

from op_analytics.coreutils.clickhouse.client import new_stateful_client
from op_analytics.coreutils.clickhouse.oplabs import insert_oplabs
from op_analytics.coreutils.logger import bound_contextvars, structlog
from op_analytics.coreutils.time import date_tostr

from .create import create_tables
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


@dataclass
class TransformTask:
    group_name: str
    dt: date

    skip_create: bool
    update_only: list[str] | None
    raise_if_empty: bool

    @property
    def context(self):
        return dict(dt=date_tostr(self.dt))

    def execute(self):
        client = new_stateful_client("OPLABS")

        with bound_contextvars(**self.context):
            if not self.skip_create:
                self.create_tables()
            results = self.run_updates(client)
            result_dicts = [_.to_dict() for _ in results]
            self.write_marker(result_dicts)
            return results

    def create_tables(self):
        create_tables(self.group_name)

    def run_updates(self, client) -> list[UpdateResult]:
        """Find the sequence of DDLs for this task and run them."""

        results = []

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
                        select_statement=step.select_statement,
                    )

        return results

    def run_update(self, client, step: Step):
        log.info(f"running ddl {step.name}")

        try:
            result: QuerySummary = client.command(
                cmd=step.sql_statement,
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
                raise Exception("possible data quality issue 0 rows were written!")

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
