from op_analytics.coreutils.logger import bound_contextvars, structlog
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.datapipeline.models.compute.runner import run_tasks

from .construct import construct_tasks

log = structlog.get_logger()


@bound_contextvars(pipeline_step="compute_intermediate")
def compute_intermediate(
    chains: list[str],
    models: list[str],
    range_spec: str,
    read_from: DataLocation,
    write_to: DataLocation,
    dryrun: bool,
    force_complete: bool = False,
    fork_process: bool = True,
):
    tasks = construct_tasks(chains, models, range_spec, read_from, write_to)

    run_tasks(
        tasks=tasks,
        dryrun=dryrun,
        force_complete=force_complete,
        fork_process=fork_process,
    )
