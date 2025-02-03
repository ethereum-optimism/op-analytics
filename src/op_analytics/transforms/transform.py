import json
import os
import socket
from dataclasses import dataclass
from datetime import date

import polars as pl

from op_analytics.coreutils.clickhouse.oplabs import insert_oplabs
from op_analytics.coreutils.logger import structlog, bound_contextvars
from op_analytics.coreutils.time import date_tostr


from .create import create_tables
from .markers import TRANSFORM_MARKERS_TABLE
from .update import run_updates, UpdateResult

log = structlog.get_logger()


@dataclass
class TransformTask:
    group_name: str
    dt: date

    skip_create: bool
    start_at_index: int

    @property
    def context(self):
        return dict(dt=date_tostr(self.dt))

    def execute(self):
        with bound_contextvars(**self.context):
            if not self.skip_create:
                self.create_tables()
            results = self.run_updates()
            result_dicts = [_.to_dict() for _ in results]
            self.write_marker(result_dicts)
            return results

    def create_tables(self):
        create_tables(self.group_name)

    def run_updates(self) -> list[UpdateResult]:
        return run_updates(self.group_name, dt=self.dt, start_at_index=self.start_at_index)

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
