import multiprocessing as mp
from typing import Protocol, Sequence

import duckdb

from op_analytics.coreutils.duckdb_inmem import init_client
from op_analytics.coreutils.duckdb_local.client import disconnect_duckdb_local
from op_analytics.coreutils.logger import (
    bind_contextvars,
    bound_contextvars,
    structlog,
)
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.coreutils.partitioned.output import OutputData
from op_analytics.coreutils.partitioned.reader import DataReader
from op_analytics.coreutils.partitioned.writehelper import WriteManager
from op_analytics.datapipeline.models.compute.modelexecute import PythonModelExecutor
from op_analytics.datapipeline.models.compute.registry import (
    REGISTERED_INTERMEDIATE_MODELS,
    load_model_definitions,
)
from op_analytics.datapipeline.models.compute.udfs import create_duckdb_macros, set_memory_limit

log = structlog.get_logger()


class ModelsTask(Protocol):
    # Model to compute
    model: str

    # DataReader
    data_reader: DataReader

    # Write Manager
    write_manager: WriteManager

    # Output duckdb relations
    output_duckdb_relations: dict[str, duckdb.DuckDBPyRelation]

    # Top directory where the results of the model will be stored.
    output_root_path_prefix: str


def run_tasks(
    tasks: Sequence[ModelsTask],
    dryrun: bool,
    force_complete: bool = False,
    fork_process: bool = True,
):
    if dryrun:
        log.info("DRYRUN: No work will be done.")
        return

    executed = 0
    executed_ok = 0
    for i, task in enumerate(tasks):
        bind_contextvars(
            task=f"{i+1}/{len(tasks)}",
            **task.data_reader.partitions_dict(),
        )

        # Decide if we need to run this task.
        if task.write_manager.is_complete() and not force_complete:
            log.info("task", status="already_complete")
            continue

        # Decide if we can run this task.
        if not task.data_reader.inputs_ready:
            log.warning("task", status="input_not_ready")
            continue

        if force_complete:
            log.info("forced execution despite complete marker")
            task.write_manager.force = True

        # If running locally release duckdb lock before forking.
        if task.write_manager.location == DataLocation.LOCAL:
            disconnect_duckdb_local()

        executed += 1
        success = execute(task, fork_process)
        if success:
            executed_ok += 1

    log.info("done", total=executed, success=executed_ok, fail=executed - executed_ok)


def execute(task: ModelsTask, fork_process: bool) -> bool:
    """Returns true if task succeeds."""
    if fork_process:
        ctx = mp.get_context("spawn")
        p = ctx.Process(target=steps, args=(task,))
        p.start()
        p.join()

        if p.exitcode != 0:
            log.error("task", status="fail", exitcode=p.exitcode)
            return False
        else:
            log.info("task", status="success", exitcode=0)
            return True
    else:
        steps(task)
        return True


def steps(task: ModelsTask) -> None:
    """Execute the model computations."""

    # Load shared DuckDB UDFs.
    client = init_client()
    create_duckdb_macros(client)

    # Set duckdb memory limit. This lets us get an error from duckb instead of
    # OOMing the container.
    set_memory_limit(client, gb=10)

    # Load models
    load_model_definitions(module_names=[task.model])

    # Get the model.
    im_model = REGISTERED_INTERMEDIATE_MODELS[task.model]

    with PythonModelExecutor(im_model, client, task.data_reader) as m:
        with bound_contextvars(model=task.model, **task.data_reader.partitions_dict()):
            log.info("running model")
            model_results = m.execute()

            produced_datasets = set(model_results.keys())
            if produced_datasets != set(im_model.expected_output_datasets):
                raise RuntimeError(
                    f"model {task.model!r} produced unexpected datasets: {produced_datasets}"
                )

            for result_name, rel in model_results.items():
                task.write_manager.write(
                    output_data=OutputData(
                        dataframe=rel.pl(),
                        root_path=f"{task.output_root_path_prefix}/{task.model}/{result_name}",
                        default_partition=task.data_reader.partitions_dict(),
                    ),
                )
