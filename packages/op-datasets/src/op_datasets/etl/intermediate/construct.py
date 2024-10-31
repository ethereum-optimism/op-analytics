from op_coreutils.logger import structlog
from op_coreutils.partitioned import DataLocation, InputData, construct_inputs


from .task import IntermediateModelsTask


log = structlog.get_logger()


def construct_tasks(
    chains: list[str],
    models: list[str],
    range_spec: str,
    read_from: DataLocation,
    write_to: list[DataLocation],
) -> list[IntermediateModelsTask]:
    """Construct a collection of tasks to compute intermediate models.

    While constructing tasks we also go ahead and load the model definitions and create the
    shared duckdb macros that are used across models.
    """

    inputs: list[InputData] = construct_inputs(
        chains=chains,
        range_spec=range_spec,
        read_from=read_from,
        input_datasets=["blocks", "transactions", "logs", "traces"],
    )

    tasks = []
    for inputdata in inputs:
        # TODO: Compute what are the expected markers for the task.

        tasks.append(
            IntermediateModelsTask(
                inputdata=inputdata,
                models=models,
                output_duckdb_relations={},
                write_to=write_to,
                force=False,
                expected_markers=[],
                is_complete=False,
            )
        )

    log.info(f"Constructed {len(tasks)} tasks.")
    return tasks
