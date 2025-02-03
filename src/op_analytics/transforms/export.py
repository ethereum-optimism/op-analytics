from clickhouse_connect.driver.summary import QuerySummary
from op_analytics.coreutils.clickhouse.client import new_stateful_client
from op_analytics.coreutils.logger import structlog, bound_contextvars
from clickhouse_connect.driver.exceptions import DatabaseError

from op_analytics.coreutils.time import date_tostr

from .steps import read_steps

log = structlog.get_logger()


def execute_exports(group_name: str, start_at_index: int = 0):
    """Run data exports.

    This creates parquet files in GCS from the configured export queries
    running on ClickHouse.

    Date exports have no concept of date partition. They run over the full
    scope of input data that is needed to produce a report.
    """
    client = new_stateful_client("OPLABS")

    summary = {}

    for step in read_steps(group_name=group_name, step_name="update"):
        with bound_contextvars(ddl=step.name):
            if step.index < start_at_index:
                log.info(f"skipping index={step.index} ddl={step.name}")
                continue

            log.info(f"running ddl {step.name}")

            try:
                result: QuerySummary = client.command(
                    cmd=step.sql_statement,
                    parameters={"dtparam": dateval},
                    settings={"use_hive_partitioning": 1},
                )
            except DatabaseError as ex:
                log.error("database error", exc_info=ex)
                raise
    summary[step.name] = result.written_rows

    # Execute transforms.

    num_tasks = len(tasks)
    for ii, task in enumerate(tasks):
        with bound_contextvars(task=f"{ii+1}/{num_tasks}"):
            result = task.execute(start_at_index=start_at_index)

        summary[date_tostr(task.dt)] = result

        if max_tasks is not None and ii + 1 >= max_tasks:
            break
    return summary
