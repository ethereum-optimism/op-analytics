from collections import defaultdict
from datetime import date

import pyarrow as pa

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.coreutils.partitioned.output import ExpectedOutput
from op_analytics.coreutils.partitioned.paths import get_dt, get_root_path
from op_analytics.coreutils.partitioned.reader import DataReader
from op_analytics.coreutils.time import date_fromstr
from op_analytics.datapipeline.etl.ingestion.reader.bydate import construct_readers_bydate
from op_analytics.datapipeline.etl.ingestion.reader.request import BlockBatchRequest
from op_analytics.datapipeline.etl.ingestion.reader.rootpaths import RootPath

from .loader import BQLoader, BQOutputData
from .task import DateLoadTask

log = structlog.get_logger()


def construct_date_load_tasks(
    chains: list[str],
    range_spec: str,
    root_paths_to_read: list[RootPath],
    write_to: DataLocation,
    markers_table: str,
    bq_dataset_name: str,
) -> list[DateLoadTask]:
    """Consolidate chain/date combinations into one task per date.

    Readers are built for each chain/date pair. We go over them collecting all the data that
    needs to be loaded from all chains for each date.
    """
    # Prepare the request for input data.
    blockbatch_request = BlockBatchRequest.build(
        chains=chains,
        range_spec=range_spec,
        root_paths_to_read=root_paths_to_read,
    )

    readers: list[DataReader] = construct_readers_bydate(
        blockbatch_request=blockbatch_request,
        read_from=DataLocation.GCS,
    )

    # For each date, keep track of datasets and parquet paths.
    date_paths: dict[date, dict[str, list]] = defaultdict(lambda: defaultdict(list))

    # For each date, keep track of chains that are ready to load.
    date_chains_ready = defaultdict(set)

    # For each date, keep track of chains that are not ready to load.
    date_chains_not_ready = defaultdict(set)

    # Collect information from all readers grouped by date.
    for inputdata in readers:
        dateval = date_fromstr(inputdata.partition_value("dt"))
        chain = inputdata.partition_value("chain")

        if inputdata.inputs_ready:
            date_chains_ready[dateval].add(chain)
        else:
            date_chains_not_ready[dateval].add(chain)

        for dataset, paths in inputdata.dataset_paths.items():
            date_paths[dateval][dataset].extend(paths)

    # For each date build a DateLoadTask.
    result: list[DateLoadTask] = []
    for dateval, dataset_paths in date_paths.items():
        #
        # We load multiple datasets into BQ (blocks, logs, transactions, traces) for each date.
        #
        # Here we produce the expected_output (needed for markers) and output (needed for loading)
        # for each date.
        #
        outputs = []
        expected_outputs = []
        for root_path, parquet_paths in dataset_paths.items():
            # Get the common root path for all the source parquet paths.
            source_uris_root_path = get_root_path(parquet_paths)
            assert source_uris_root_path.endswith(root_path + "/")

            # Get the common date partition for all the source parquet paths
            # and make sure it agrees with the task.
            dateval_from_paths = get_dt(parquet_paths)
            assert dateval == dateval_from_paths

            # Compute the BQ table name from the root path. For example:
            # root_path = "ingestion/traces_v1"  --> traces
            bq_table_name = root_path.split("/")[-1].split("_")[0]

            bq_root_path = f"{bq_dataset_name}/{bq_table_name}"
            expected_outputs.append(
                ExpectedOutput(
                    root_path=bq_root_path,
                    file_name="",  # Not meaningful for BQ Load
                    marker_path=f"{bq_dataset_name}/{bq_table_name}/{dateval.strftime("%Y-%m-%d")}",
                )
            )

            outputs.append(
                BQOutputData(
                    root_path=bq_root_path,
                    source_uris=parquet_paths,
                    source_uris_root_path=source_uris_root_path,
                    dateval=dateval,
                    bq_dataset_name=bq_dataset_name,
                    bq_table_name=bq_table_name,
                )
            )

        result.append(
            DateLoadTask(
                dateval=dateval,
                chains_ready=date_chains_ready[dateval],
                chains_not_ready=date_chains_not_ready[dateval],
                write_manager=BQLoader(
                    location=write_to,
                    partition_cols=["dt"],
                    extra_marker_columns={},
                    extra_marker_columns_schema=[
                        pa.field("dt", pa.date32()),
                    ],
                    markers_table=markers_table,
                    expected_outputs=expected_outputs,
                ),
                outputs=outputs,
            )
        )

    log.info(f"Consolidated to {len(result)} dateval tasks.")
    return result
