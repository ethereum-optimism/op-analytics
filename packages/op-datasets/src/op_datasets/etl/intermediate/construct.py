import pyarrow as pa

from op_coreutils.logger import structlog
from op_coreutils.partitioned import (
    DataLocation,
    DataReader,
    construct_input_batches,
    SinkMarkerPath,
    SinkOutputRootPath,
    DataWriter,
    ExpectedOutput,
)

from op_datasets.etl.ingestion.markers import INGESTION_DATASETS, INGESTION_MARKERS_TABLE

from .markers import INTERMEDIATE_MODELS_MARKERS_TABLE
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

    batches: list[DataReader] = construct_input_batches(
        chains=chains,
        range_spec=range_spec,
        read_from=read_from,
        markers_table=INGESTION_MARKERS_TABLE,
        dataset_names=INGESTION_DATASETS,
    )

    tasks = []
    for batch in batches:
        # Each model can have one or more outputs. There is 1 marker per output.
        expected_outputs = {}
        for model in models:
            for dataset in REGISTERED_INTERMEDIATE_MODELS[model].expected_output_datasets:
                dataset_name = f"{model}/{dataset}"

                expected_outputs[dataset_name] = ExpectedOutput(
                    dataset_name=dataset_name,
                    root_path=SinkOutputRootPath(f"intermediate/{dataset_name}"),
                    file_name="out.parquet",
                    marker_path=SinkMarkerPath(dataset_name),
                    process_name="default",
                    additional_columns=dict(
                        mode_name=model,
                    ),
                    additional_columns_schema=[
                        pa.field("chain", pa.string()),
                        pa.field("dt", pa.date32()),
                        pa.field("model_name", pa.string()),
                    ],
                )

        tasks.append(
            IntermediateModelsTask(
                data_reader=batch,
                models=models,
                output_duckdb_relations={},
                data_writer=DataWriter(
                    write_to=write_to,
                    markers_table=INTERMEDIATE_MODELS_MARKERS_TABLE,
                    expected_outputs=expected_outputs,
                    is_complete=False,
                    force=False,
                ),
            )
        )

    log.info(f"Constructed {len(tasks)} tasks.")
    return tasks
