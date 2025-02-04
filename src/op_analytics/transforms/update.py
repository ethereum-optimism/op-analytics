from dataclasses import asdict, dataclass
from datetime import date

from clickhouse_connect.driver.summary import QuerySummary
from op_analytics.coreutils.clickhouse.client import new_stateful_client
from op_analytics.coreutils.logger import structlog, bound_contextvars
from clickhouse_connect.driver.exceptions import DatabaseError

from .steps import read_steps

log = structlog.get_logger()


@dataclass
class UpdateResult:
    name: str
    result: dict

    def to_dict(self):
        return asdict(self)


def run_updates(
    group_name: str,
    dt: date,
    start_at_index: int,
    raise_if_empty: bool,
) -> list[UpdateResult]:
    """Find the sequence of DDLs for this task and run them."""

    client = new_stateful_client("OPLABS")
    results = []

    for step in read_steps(group_name=group_name, step_name="update"):
        with bound_contextvars(ddl=step.name):
            if step.index < start_at_index:
                log.info(f"skipping index={step.index} ddl={step.name}")
                continue

            log.info(f"running ddl {step.name}")

            try:
                result: QuerySummary = client.command(
                    cmd=step.sql_statement,
                    parameters={"dtparam": dt},
                    settings={"use_hive_partitioning": 1},
                )
            except DatabaseError as ex:
                log.error("database error", exc_info=ex)
                raise

            assert isinstance(result.summary, dict)
            log.info(f"{step.name} -> {result.written_rows} written rows", **result.summary)

            if result.written_rows == 0 and raise_if_empty:
                raise Exception("possible data quality issue 0 rows were written!")

            results.append(
                UpdateResult(
                    name=step.name,
                    result=result.summary,
                )
            )

    return results
