from dagster import OpExecutionContext

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.rangeutils.daterange import DateRange
from op_analytics.coreutils.threads import run_concurrently_store_failures
from op_analytics.datapipeline.chains.goldsky_chains import goldsky_mainnet_chains

from .loadspec import ClickHouseDailyDataset
from .insert import DtChainBatch, InsertTask
from .markers import query_blockbatch_daily_markers
from .readers import construct_batches

log = structlog.get_logger()


def daily_to_clickhouse(
    dataset: ClickHouseDailyDataset,
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

    chains = chains or goldsky_mainnet_chains()

    ready_batches: list[DtChainBatch] = construct_batches(
        range_spec=range_spec,
        chains=chains,
        blockbatch_root_paths=dataset.inputs_blockbatch,
        clickhouse_root_paths=dataset.inputs_clickhouse,
    )

    # Existing markers that have already been loaded to ClickHouse.
    date_range = DateRange.from_spec(range_spec)
    existing_markers_df = query_blockbatch_daily_markers(
        date_range=date_range,
        chains=chains,
        root_paths=[dataset.output_root_path],
    )
    existing_markers = set(
        DtChainBatch.of(chain=x["chain"], dt=x["dt"]) for x in existing_markers_df.to_dicts()
    )

    # Loop over batches and find which ones are pending.
    tasks: list[InsertTask] = []
    for batch in ready_batches:
        if batch in existing_markers:
            continue
        tasks.append(InsertTask(dataset=dataset, batch=batch))
    log.info(f"{len(tasks)}/{len(ready_batches)} pending dt,chain insert tasks.")

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
