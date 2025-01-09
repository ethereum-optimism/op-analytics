import pyarrow as pa

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dataaccess import check_marker
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.coreutils.partitioned.output import ExpectedOutput
from op_analytics.coreutils.partitioned.reader import DataReader
from op_analytics.coreutils.partitioned.writer import PartitionedWriteManager
from op_analytics.datapipeline.chains.goldsky_chains import ChainNetwork, determine_network
from op_analytics.datapipeline.etl.ingestion.reader.byblock import construct_readers_byblock
from op_analytics.datapipeline.etl.ingestion.reader.request import BlockBatchRequest
from op_analytics.datapipeline.etl.ingestion.reader.rootpaths import RootPath
from op_analytics.datapipeline.models.compute.model import PythonModel

from .reader.markers import BLOCKBATCH_MARKERS_TABLE, make_data_spec

from .task import BlockBatchModelsTask

log = structlog.get_logger()


def construct_data_readers(
    blockbatch_request: BlockBatchRequest,
    models: list[str],
    read_from: DataLocation,
) -> list[DataReader]:
    """Construct data readers for a list of models.

    This function is exposed only so it can be used as part of a notebook.
    """
    model_objs = [PythonModel.get(_) for _ in models]

    input_datasets: set[RootPath] = set()
    for model_obj in model_objs:
        for input_dataset in model_obj.input_datasets:
            input_datasets.add(RootPath.of(input_dataset))

    return construct_readers_byblock(
        blockbatch_request=blockbatch_request,
        read_from=read_from,
        root_paths_to_read=sorted(input_datasets),
    )


def construct_tasks(
    chains: list[str],
    models: list[str],
    range_spec: str,
    read_from: DataLocation,
    write_to: DataLocation,
) -> list[BlockBatchModelsTask]:
    """Construct a collection of tasks to compute intermediate models."""
    # Prepare the request.
    blockbatch_request = BlockBatchRequest.build(chains, range_spec)

    # Prepare data readers.
    readers: list[DataReader] = construct_data_readers(
        blockbatch_request=blockbatch_request,
        models=models,
        read_from=read_from,
    )

    # Pre-fetch completion markers so we can skip completed tasks.
    data_spec = make_data_spec(chains=chains, models=models)
    output_markers_df = data_spec.query_markers(
        datevals=blockbatch_request.datevals,
        location=write_to,
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
                full_output_name = f"{model_name}/{dataset}"

                datestr = reader.partition_value("dt")
                chain = reader.partition_value("chain")

                network = determine_network(chain)
                if network == ChainNetwork.TESTNET:
                    root_path_prefix = "blockbatch_testnets"
                else:
                    root_path_prefix = "blockbatch"

                min_block = reader.marker_data("min_block")
                min_block_str = f"{min_block:012d}"

                eo = ExpectedOutput(
                    root_path=f"{root_path_prefix}/{full_output_name}",
                    file_name=f"{min_block_str}.parquet",
                    marker_path=f"{root_path_prefix}/{full_output_name}/{chain}/{datestr}/{min_block_str}",
                )
                expected_outputs.append(eo)

                if check_marker(markers_df=output_markers_df, marker_path=eo.marker_path):
                    complete_markers.append(eo.marker_path)

            tasks.append(
                BlockBatchModelsTask(
                    data_reader=reader,
                    model=model_obj,
                    output_duckdb_relations={},
                    write_manager=PartitionedWriteManager(
                        location=write_to,
                        partition_cols=["chain", "dt"],
                        extra_marker_columns=dict(
                            model_name=model_name,
                            num_blocks=reader.marker_data("num_blocks"),
                            min_block=reader.marker_data("min_block"),
                            max_block=reader.marker_data("max_block"),
                        ),
                        extra_marker_columns_schema=[
                            pa.field("chain", pa.string()),
                            pa.field("dt", pa.date32()),
                            pa.field("num_blocks", pa.int32()),
                            pa.field("min_block", pa.int64()),
                            pa.field("max_block", pa.int64()),
                            pa.field("model_name", pa.string()),
                        ],
                        markers_table=BLOCKBATCH_MARKERS_TABLE,
                        expected_outputs=expected_outputs,
                        complete_markers=complete_markers,
                    ),
                    output_root_path_prefix=root_path_prefix,
                )
            )

    log.info(f"constructed {len(tasks)} tasks.")
    return tasks
