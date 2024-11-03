from op_coreutils.logger import structlog
from op_coreutils.partitioned import DataLocation, InputData, construct_inputs, SinkMarkerPath

from op_datasets.etl.ingestion.markers import INGESTION_DATASETS, INGESTION_MARKERS_TABLE

from .registry import REGISTERED_INTERMEDIATE_MODELS
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
        markers_table=INGESTION_MARKERS_TABLE,
        dataset_names=INGESTION_DATASETS,
    )

    tasks = []
    for inputdata in inputs:
        # There is 1 marker per output dataset.
        expected_markers = []
        for model in models:
            for dataset in REGISTERED_INTERMEDIATE_MODELS[model].expected_output_datasets:
                expected_markers.append(SinkMarkerPath(f"{model}/{dataset}"))

        tasks.append(
            IntermediateModelsTask(
                inputdata=inputdata,
                models=models,
                output_duckdb_relations={},
                write_to=write_to,
                force=False,
                expected_markers=expected_markers,
                is_complete=False,
            )
        )

    log.info(f"Constructed {len(tasks)} tasks.")
    return tasks
