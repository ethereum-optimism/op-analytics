import pyarrow as pa


from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.coreutils.partitioned.output import ExpectedOutput
from op_analytics.coreutils.partitioned.reader import DataReader
from op_analytics.coreutils.partitioned.writer import DataWriter
from op_analytics.datapipeline.etl.ingestion.markers import (
    INGESTION_MARKERS_TABLE,
)
from op_analytics.datapipeline.etl.ingestion.reader import construct_readers
from op_analytics.datapipeline.chains.goldsky_chains import determine_network, ChainNetwork
from op_analytics.datapipeline.models.compute.registry import (
    REGISTERED_INTERMEDIATE_MODELS,
    load_model_definitions,
)

from .task import IntermediateModelsTask

log = structlog.get_logger()

INTERMEDIATE_MODELS_MARKERS_TABLE = "intermediate_model_markers"


def construct_tasks(
    chains: list[str],
    models: list[str],
    range_spec: str,
    read_from: DataLocation,
    write_to: DataLocation,
) -> list[IntermediateModelsTask]:
    """Construct a collection of tasks to compute intermediate models.

    While constructing tasks we also go ahead and load the model definitions and create the
    shared duckdb macros that are used across models.
    """
    # Load python functions that define registered data models.
    load_model_definitions()

    for model in models:
        should_exit = False
        if model not in REGISTERED_INTERMEDIATE_MODELS:
            should_exit = True
            log.error(f"Model is not registered: {model}")
        if should_exit:
            log.error("Cannot run on unregistered models. Will exit.")
            exit(1)

    input_datasets = set()
    for model in models:
        input_datasets.update(REGISTERED_INTERMEDIATE_MODELS[model].input_datasets)

    readers: list[DataReader] = construct_readers(
        chains=chains,
        range_spec=range_spec,
        read_from=read_from,
        markers_table=INGESTION_MARKERS_TABLE,
        root_paths=sorted(input_datasets),
    )

    tasks = []
    for reader in readers:
        for model in models:
            # Each model can have one or more outputs. There is 1 marker per output.
            expected_outputs = []
            for dataset in REGISTERED_INTERMEDIATE_MODELS[model].expected_output_datasets:
                full_model_name = f"{model}/{dataset}"

                datestr = reader.partition_value("dt")
                chain = reader.partition_value("chain")

                network = determine_network(chain)
                if network == ChainNetwork.TESTNET:
                    root_path_prefix = "intermediate_testnets"
                else:
                    root_path_prefix = "intermediate"

                expected_outputs.append(
                    ExpectedOutput(
                        root_path=f"{root_path_prefix}/{full_model_name}",
                        file_name="out.parquet",
                        marker_path=f"{datestr}/{chain}/{model}/{dataset}",
                        process_name="default",
                        additional_columns=dict(
                            model_name=model,
                        ),
                        additional_columns_schema=[
                            pa.field("chain", pa.string()),
                            pa.field("dt", pa.date32()),
                            pa.field("model_name", pa.string()),
                        ],
                    )
                )

            tasks.append(
                IntermediateModelsTask(
                    data_reader=reader,
                    model=model,
                    output_duckdb_relations={},
                    data_writer=DataWriter(
                        partition_cols=["chain", "dt"],
                        write_to=write_to,
                        markers_table=INTERMEDIATE_MODELS_MARKERS_TABLE,
                        expected_outputs=expected_outputs,
                        force=False,
                    ),
                    root_path_prefix=root_path_prefix,
                )
            )

    log.info(f"constructed {len(tasks)} tasks.")
    return tasks
