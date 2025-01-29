import pyarrow as pa

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dataaccess import check_marker
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.coreutils.partitioned.output import ExpectedOutput
from op_analytics.coreutils.partitioned.reader import DataReader
from op_analytics.coreutils.partitioned.writer import PartitionedWriteManager
from op_analytics.datapipeline.chains.goldsky_chains import ChainNetwork, determine_network
from op_analytics.datapipeline.etl.ingestion.reader.bydate import construct_readers_bydate
from op_analytics.datapipeline.etl.ingestion.reader.request import BlockBatchRequest
from op_analytics.datapipeline.models.compute.execute import PythonModel
from op_analytics.datapipeline.models.compute.markers import ModelsDataSpec

from .task import IntermediateModelsTask

log = structlog.get_logger()


INTERMEDIATE_MARKERS_TABLE = "intermediate_model_markers"


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
    data_spec = ModelsDataSpec(root_path_prefix="intermediate", models=models)

    # Prepare the request for input data.
    blockbatch_request = BlockBatchRequest.build(
        chains=chains,
        range_spec=range_spec,
        root_paths_to_read=data_spec.input_root_paths,
    )

    readers: list[DataReader] = construct_readers_bydate(
        blockbatch_request=blockbatch_request,
        read_from=read_from,
    )

    # Prepare a request for output data to pre-fetch completion markers.
    # Markers are used to skip already completed tasks.
    output_blockbatch_request = BlockBatchRequest.build(
        chains=chains,
        range_spec=range_spec,
        root_paths_to_read=data_spec.output_root_paths,
    )
    output_markers_df = output_blockbatch_request.query_markers(
        location=write_to,
        markers_table=INTERMEDIATE_MARKERS_TABLE,
        extra_columns=[],
    )

    unique_chains = output_markers_df["chain"].n_unique()
    log.info(f"pre-fetched {len(output_markers_df)} markers for {unique_chains} chains")

    model_objs = [PythonModel.get(_) for _ in models]
    tasks = []
    for reader in readers:
        for model_obj in model_objs:
            model_name = model_obj.name

            # Each model can have one or more outputs. There is 1 marker per output.
            expected_outputs = []
            complete_markers: list[str] = []

            for dataset in model_obj.expected_output_datasets:
                full_model_name = f"{model_name}/{dataset}"

                datestr = reader.partition_value("dt")
                chain = reader.partition_value("chain")

                network = determine_network(chain)
                if network == ChainNetwork.TESTNET:
                    root_path_prefix = "intermediate_testnets"
                else:
                    root_path_prefix = "intermediate"

                eo = ExpectedOutput(
                    root_path=f"{root_path_prefix}/{full_model_name}",
                    file_name="out.parquet",
                    marker_path=f"{datestr}/{chain}/{model_name}/{dataset}",
                )
                expected_outputs.append(eo)

                if check_marker(markers_df=output_markers_df, marker_path=eo.marker_path):
                    complete_markers.append(eo.marker_path)

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
                        markers_table=INTERMEDIATE_MARKERS_TABLE,
                        expected_outputs=expected_outputs,
                        complete_markers=complete_markers,
                    ),
                    output_root_path_prefix=root_path_prefix,
                )
            )

    log.info(f"constructed {len(tasks)} tasks.")
    return tasks
