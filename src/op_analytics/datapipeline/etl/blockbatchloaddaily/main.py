from dagster import OpExecutionContext

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.threads import run_concurrently_store_failures

from .loadspec_datechain import ClickHouseDateChainETL
from .insert import InsertTask

log = structlog.get_logger()


def daily_to_clickhouse(
    dataset: ClickHouseDateChainETL,
    range_spec: str | None = None,
    dry_run: bool = False,
    num_workers: int = 1,
    chains: list[str] | None = None,
    reverse: bool = True,
    dagster_context: OpExecutionContext | None = None,
):
    """Insert blockbatch data into Clickhouse at a dt,chain granularity."""

    # Operate over recent days.
    range_spec = range_spec or "m4days"

    pending_batches = dataset.pending_batches(range_spec=range_spec, chains=chains)

    tasks: list[InsertTask] = []
    for batch in pending_batches:
        tasks.append(InsertTask(dataset=dataset, batch=batch))
    log.info(f"{len(tasks)}/{len(pending_batches)} pending dt,chain insert tasks.")

    # Sort tasks by date.
    tasks.sort(key=lambda x: x.batch.dt, reverse=reverse)
    if dry_run:
        tasks[0].dry_run()
        log.warning("DRY RUN: Only the first task is shown.")
        return

    # Create the output tables if they don't exist.
    dataset.create_table()

    # Run the tasks.
    summary = run_concurrently_store_failures(
        function=lambda x: x.execute(),
        targets={t.batch.partitioned_path: t for t in tasks},
        max_workers=num_workers,
    )

    if summary.failures:
        for task_id, error_msg in summary.failures.items():
            detail = f"failed task={task_id}: error={error_msg}"
            log.error(detail)
            if dagster_context:
                dagster_context.log.error(detail)

        msg = f"output={dataset.output_root_path}: {len(summary.failures)} chain,dt tasks failed to execute"
        log.error(msg)
        if dagster_context:
            dagster_context.log.error(msg)
        raise Exception(msg)

    return summary.results
