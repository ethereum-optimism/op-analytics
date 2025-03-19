from op_analytics.coreutils.logger import (
    bound_contextvars,
    structlog,
)
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.datapipeline.chains.goldsky_chains import goldsky_mainnet_chains
from op_analytics.datapipeline.etl.ingestion.reader.rootpaths import RootPath

from .construct import construct_date_load_tasks
from .task import DateLoadTask

log = structlog.get_logger()


def load_blockbatch_to_bq(
    range_spec: str,
    root_paths_to_read: list[RootPath],
    bq_dataset_name: str,
    table_name_map: dict[str, str],
    markers_table: str,
    dryrun: bool,
    force_complete: bool,
    force_not_ready: bool,
    excluded_chains: list[str] | None = None,
):
    all_chains = goldsky_mainnet_chains()

    chains = []
    excluded_chains = excluded_chains or []
    for chain in all_chains:
        if chain not in excluded_chains:
            chains.append(chain)

    if dryrun:
        log.info("DRYRUN: No work will be done.")
        return

    date_tasks: list[DateLoadTask] = construct_date_load_tasks(
        chains=chains,
        range_spec=range_spec,
        root_paths_to_read=root_paths_to_read,
        write_to=DataLocation.BIGQUERY,
        markers_table=markers_table,
        bq_dataset_name=bq_dataset_name,
        table_name_map=table_name_map,
    )

    success = 0
    for i, task in enumerate(date_tasks):
        with bound_contextvars(
            task=f"{i + 1}/{len(date_tasks)}",
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

    return dict(prepared=len(date_tasks), success=success)
