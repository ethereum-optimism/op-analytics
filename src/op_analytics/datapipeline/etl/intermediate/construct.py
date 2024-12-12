import pyarrow as pa


from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.coreutils.partitioned.output import ExpectedOutput
from op_analytics.coreutils.partitioned.reader import DataReader
from op_analytics.coreutils.partitioned.writer import PartitionedWriteManager
from op_analytics.datapipeline.etl.ingestion.reader_bydate import construct_readers_bydate
from op_analytics.datapipeline.chains.goldsky_chains import determine_network, ChainNetwork
from op_analytics.datapipeline.models.compute.modelexecute import PythonModel

from .task import IntermediateModelsTask

log = structlog.get_logger()

INTERMEDIATE_MODELS_MARKERS_TABLE = "intermediate_model_markers"


def construct_data_readers(
    chains: list[str],
    models: list[str],
    range_spec: str,
    read_from: DataLocation,
) -> list[DataReader]:
    model_objs = [PythonModel.get(_) for _ in models]

    input_datasets = set()
    for _ in model_objs:
        input_datasets.update(_.input_datasets)

    return construct_readers_bydate(
        chains=chains,
        range_spec=range_spec,
        read_from=read_from,
        root_paths_to_read=sorted(input_datasets),
    )


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
    readers: list[DataReader] = construct_data_readers(
        chains=chains,
        models=models,
        range_spec=range_spec,
        read_from=read_from,
    )

    model_objs = [PythonModel.get(_) for _ in models]

    tasks = []
    for reader in readers:
        for model_obj in model_objs:
            model_name = model_obj.name

            # Each model can have one or more outputs. There is 1 marker per output.
            expected_outputs = []
            for dataset in model_obj.expected_output_datasets:
                full_model_name = f"{model_name}/{dataset}"

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
                        marker_path=f"{datestr}/{chain}/{model_name}/{dataset}",
                    )
                )

            tasks.append(
                IntermediateModelsTask(
                    data_reader=reader,
                    model=model_obj,
                    output_duckdb_relations={},
                    write_manager=PartitionedWriteManager(
                        location=write_to,
                        partition_cols=["chain", "dt"],
                        extra_marker_columns=dict(
                            model_name=model_name,
                        ),
                        extra_marker_columns_schema=[
                            pa.field("chain", pa.string()),
                            pa.field("dt", pa.date32()),
                            pa.field("model_name", pa.string()),
                        ],
                        markers_table=INTERMEDIATE_MODELS_MARKERS_TABLE,
                        expected_outputs=expected_outputs,
                    ),
                    output_root_path_prefix=root_path_prefix,
                )
            )

    log.info(f"constructed {len(tasks)} tasks.")
    return tasks
