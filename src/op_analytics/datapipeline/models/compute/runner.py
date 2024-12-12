import concurrent.futures
import multiprocessing as mp
from dataclasses import dataclass
from typing import Generator, Protocol, Sequence

import duckdb

from op_analytics.coreutils.duckdb_inmem import init_client
from op_analytics.coreutils.duckdb_local.client import disconnect_duckdb_local
from op_analytics.coreutils.logger import (
    bound_contextvars,
    structlog,
)
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.coreutils.partitioned.output import OutputData
from op_analytics.coreutils.partitioned.reader import DataReader
from op_analytics.coreutils.partitioned.writehelper import WriteManager
from op_analytics.datapipeline.models.compute.modelexecute import PythonModel, PythonModelExecutor
from op_analytics.datapipeline.models.compute.udfs import create_duckdb_macros, set_memory_limit

log = structlog.get_logger()


class ModelsTask(Protocol):
    # Model to compute
    model: PythonModel

    # DataReader
    data_reader: DataReader

    # Write Manager
    write_manager: WriteManager

    # Output duckdb relations
    output_duckdb_relations: dict[str, duckdb.DuckDBPyRelation]

    # Top directory where the results of the model will be stored.
    output_root_path_prefix: str


@dataclass
class WorkItem:
    task: ModelsTask
    index: int
    total: int

    @property
    def progress(self):
        return f"{self.index+1}/{self.total}"

    def context(self):
        return dict(
            model=self.task.model.name,
            task=self.progress,
            **self.task.data_reader.partitions_dict(),
        )


def run_tasks(
    tasks: Sequence[ModelsTask],
    dryrun: bool,
    force_complete: bool = False,
    fork_process: bool = True,
    num_processes: int = 1,
):
    if dryrun:
        log.info("DRYRUN: No work will be done.")
        return

    executed = 0
    executed_ok = 0

    if fork_process:
        ctx = mp.get_context("spawn")
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=num_processes,
            mp_context=ctx,
            # NOTE: I tried using max tasks to avoid memory build up on reused executors
            # but every time I tried it execution ran into a deadlock that I couldn't
            # debug. So not enabling it for now.
            # max_tasks_per_child=20,
        ) as executor:
            futures = {}
            for item in pending_items(tasks, force_complete=force_complete):
                future = executor.submit(steps, item)
                futures[future] = item.progress
                executed += 1

            for future in concurrent.futures.as_completed(futures):
                key = futures[future]
                try:
                    future.result()
                    executed_ok += 1
                except Exception as ex:
                    log.error(f"Failed to run process for {key}", exc_info=ex)

    else:
        for item in pending_items(tasks, force_complete=force_complete):
            steps(item)
            executed_ok += 1

    log.info("done", total=executed, success=executed_ok, fail=executed - executed_ok)


def pending_items(
    tasks: Sequence[ModelsTask], force_complete: bool
) -> Generator[WorkItem, None, None]:
    """Yield only work items that need to be executed."""
    for i, task in enumerate(tasks):
        item = WorkItem(
            task=task,
            index=i,
            total=len(tasks),
        )

        with bound_contextvars(**item.context()):
            # Decide if we can run this task.
            if not task.data_reader.inputs_ready:
                log.warning("task", status="input_not_ready")
                continue

            # Decide if we need to run this task.
            # TODO: remove side effects from all_outputs_complete()
            if not force_complete and task.write_manager.all_outputs_complete():
                if not force_complete:
                    log.info("task", status="already_complete")
                    continue
                else:
                    log.info("forced execution despite complete markers")

            # If running locally release duckdb lock before forking.
            if task.write_manager.location == DataLocation.LOCAL:
                disconnect_duckdb_local()

        yield item


def steps(item: WorkItem) -> None:
    """Execute the model computations."""
    with bound_contextvars(**item.context()):
        # Load shared DuckDB UDFs.
        client = init_client()
        create_duckdb_macros(client)

        # Set duckdb memory limit. This lets us get an error from duckb instead of
        # OOMing the container.
        set_memory_limit(client, gb=10)

        task = item.task

        with PythonModelExecutor(task.model, client, task.data_reader) as m:
            log.info("running model")
            model_results = m.execute()

            produced_datasets = set(model_results.keys())
            if produced_datasets != set(task.model.expected_output_datasets):
                raise RuntimeError(
                    f"model {task.model!r} produced unexpected datasets: {produced_datasets}"
                )

            for result_name, rel in model_results.items():
                task.write_manager.write(
                    output_data=OutputData(
                        dataframe=rel.pl(),
                        root_path=f"{task.output_root_path_prefix}/{task.model.name}/{result_name}",
                        default_partition=task.data_reader.partitions_dict(),
                    ),
                )
