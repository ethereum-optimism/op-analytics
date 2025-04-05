from typing import Any

from op_analytics.coreutils.logger import bound_contextvars, structlog
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.datapipeline.models.compute.runner import run_tasks

from .construct import construct_tasks

log = structlog.get_logger()


def compute_blockbatch(
    chains: list[str],
    models: list[str],
    range_spec: str,
    read_from: DataLocation,
    write_to: DataLocation,
    dryrun: bool,
    force_complete: bool = False,
    fork_process: bool = True,
    use_pool: bool = False,
    raise_on_failures: bool = True,
) -> list[dict[str, Any]]:
    layers = []
    for ii, tasks in enumerate(construct_tasks(chains, models, range_spec, read_from, write_to)):
        with bound_contextvars(pipeline_step="blockbatch", graph_depth=ii):
            layer_result: dict[str, Any] = {}
            layer_result["depth"] = ii
            layer_result["models"] = set(_.model.name for _ in tasks)

            task_results = run_tasks(
                tasks=tasks,
                dryrun=dryrun,
                force_complete=force_complete,
                fork_process=fork_process,
                num_processes=4,
                use_pool=use_pool,
                raise_on_failures=raise_on_failures,
            )

            for key, val in task_results.items():
                layer_result[key] = val

            layers.append(layer_result)
    return layers
