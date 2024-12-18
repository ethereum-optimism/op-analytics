from op_analytics.coreutils.logger import (
    bound_contextvars,
    structlog,
)
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.datapipeline.chains.goldsky_chains import goldsky_mainnet_chains

from .construct import construct_date_load_tasks
from .task import DateLoadTask

log = structlog.get_logger()


BQ_PUBLIC_DATASET = "superchain_raw"


@bound_contextvars(pipeline_step="load_superchain_raw_to_bq")
def load_superchain_raw_to_bq(
    location: DataLocation,
    range_spec: str,
    dryrun: bool,
    force_complete: bool,
    force_not_ready: bool,
):
    # IMPORTANT: When loading to BigQuery we always load all the chains at once.
    # We do this because loading implies truncating any existing data in the date
    # partition.
    location.ensure_biguqery()

    chains = goldsky_mainnet_chains()

    if dryrun:
        log.info("DRYRUN: No work will be done.")
        return

    date_tasks: list[DateLoadTask] = construct_date_load_tasks(
        chains=chains,
        range_spec=range_spec,
        write_to=location,
        bq_dataset_name=BQ_PUBLIC_DATASET,
    )

    success = 0
    for i, task in enumerate(date_tasks):
        with bound_contextvars(
            task=f"{i+1}/{len(date_tasks)}",
            **task.contextvars,
        ):
            if task.chains_not_ready:
                log.warning("task", status="input_not_ready")
                log.warning(
                    f"some chains are not ready to load to bq: {sorted(task.chains_not_ready)}"
                )

            if task.chains_not_ready and not force_not_ready:
                log.warning("task", status="input_not_ready")
                continue

            if task.write_manager.all_outputs_complete():
                if not force_complete:
                    log.info("task", status="already_complete")
                    continue
                else:
                    task.write_manager.clear_complete_markers()
                    log.info("forced execution despite complete markers")

            for output in task.outputs:
                write_result = task.write_manager.write(output)

                log.info("task", status=write_result.status)
                success += 1
