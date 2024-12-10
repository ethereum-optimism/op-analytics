import pyarrow as pa

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.coreutils.partitioned.output import ExpectedOutput
from op_analytics.coreutils.partitioned.reader import DataReader
from op_analytics.coreutils.partitioned.writer import PartitionedWriteManager
from op_analytics.datapipeline.chains.goldsky_chains import ChainNetwork, determine_network
from op_analytics.datapipeline.etl.ingestion.reader_byblock import construct_readers_byblock
from op_analytics.datapipeline.models.compute.registry import (
    REGISTERED_INTERMEDIATE_MODELS,
    load_model_definitions,
    vefify_models,
)

from .task import BlockBatchModelsTask

log = structlog.get_logger()


BLOCKBATCH_MODELS_MARKERS_TABLE = "blockbatch_model_markers"


def construct_tasks(
    chains: list[str],
    models: list[str],
    range_spec: str,
    read_from: DataLocation,
    write_to: DataLocation,
) -> list[BlockBatchModelsTask]:
    """Construct a collection of tasks to compute intermediate models."""
    # Load python functions that define registered data models.
    load_model_definitions(module_names=models)
    vefify_models(models)

    input_datasets = set()
    for model in models:
        input_datasets.update(REGISTERED_INTERMEDIATE_MODELS[model].input_datasets)

    readers: list[DataReader] = construct_readers_byblock(
        chains=chains,
        range_spec=range_spec,
        read_from=read_from,
        root_paths_to_read=sorted(input_datasets),
    )

    tasks = []
    for reader in readers:
        assert reader.extra_marker_data is not None

        for model in models:
            # Each model can have one or more outputs. There is 1 marker per output.
            expected_outputs = []
            for dataset in REGISTERED_INTERMEDIATE_MODELS[model].expected_output_datasets:
                full_model_name = f"{model}/{dataset}"

                datestr = reader.partition_value("dt")
                chain = reader.partition_value("chain")

                network = determine_network(chain)
                if network == ChainNetwork.TESTNET:
                    root_path_prefix = "blockbatch_testnets"
                else:
                    root_path_prefix = "blockbatch"

                min_block = reader.extra_marker_data["min_block"]
                min_block_str = f"{min_block:012d}"

                expected_outputs.append(
                    ExpectedOutput(
                        root_path=f"{root_path_prefix}/{full_model_name}",
                        file_name=f"{min_block_str}.parquet",
                        marker_path=f"{root_path_prefix}/{full_model_name}/{chain}/{datestr}/{min_block_str}",
                    )
                )

            tasks.append(
                BlockBatchModelsTask(
                    data_reader=reader,
                    model=model,
                    output_duckdb_relations={},
                    write_manager=PartitionedWriteManager(
                        location=write_to,
                        partition_cols=["chain", "dt"],
                        extra_marker_columns=dict(
                            model_name=model,
                            num_blocks=reader.extra_marker_data["num_blocks"],
                            min_block=reader.extra_marker_data["min_block"],
                            max_block=reader.extra_marker_data["max_block"],
                        ),
                        extra_marker_columns_schema=[
                            pa.field("chain", pa.string()),
                            pa.field("dt", pa.date32()),
                            pa.field("num_blocks", pa.int32()),
                            pa.field("min_block", pa.int64()),
                            pa.field("max_block", pa.int64()),
                            pa.field("model_name", pa.string()),
                        ],
                        markers_table=BLOCKBATCH_MODELS_MARKERS_TABLE,
                        expected_outputs=expected_outputs,
                        force=False,
                    ),
                    output_root_path_prefix=root_path_prefix,
                )
            )

    log.info(f"constructed {len(tasks)} tasks.")
    return tasks
