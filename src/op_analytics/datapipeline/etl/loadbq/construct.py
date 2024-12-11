from collections import defaultdict
from datetime import date

import pyarrow as pa

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.coreutils.partitioned.reader import DataReader
from op_analytics.coreutils.partitioned.output import ExpectedOutput
from op_analytics.coreutils.time import date_fromstr
from op_analytics.coreutils.partitioned.paths import get_dt, get_root_path
from op_analytics.datapipeline.etl.ingestion.reader_bydate import construct_readers_bydate

from .loader import BQLoader
from .task import DateLoadTask


log = structlog.get_logger()

MARKERS_TABLE = "superchain_raw_bigquery_markers"


def construct_date_load_tasks(
    chains: list[str],
    range_spec: str,
    write_to: DataLocation,
    bq_dataset_name: str,
    force_complete: bool,
) -> list[DateLoadTask]:
    """Consolidate inputs.

    list[InputData] has separate entries for each chain and date. This function goes over
    it and collects a single DateLoadTask which covers all chains.
    """
    readers: list[DataReader] = construct_readers_bydate(
        chains=chains,
        range_spec=range_spec,
        read_from=DataLocation.GCS,
    )

    # For each date, keep track of datasets and parquet paths.
    date_paths: dict[date, dict[str, list]] = defaultdict(lambda: defaultdict(list))

    # For each date, keep track of chains that are ready to load.
    date_chains_ready = defaultdict(set)

    # For each date, keep track of chains that are not ready to load.
    date_chains_not_ready = defaultdict(set)

    for inputdata in readers:
        dateval = date_fromstr(inputdata.partition_value("dt"))
        chain = inputdata.partition_value("chain")

        if inputdata.inputs_ready:
            date_chains_ready[dateval].add(chain)
        else:
            date_chains_not_ready[dateval].add(chain)

        for dataset, paths in inputdata.dataset_paths.items():
            date_paths[dateval][dataset].extend(paths)

    result: list[DateLoadTask] = []
    for dateval, dataset_paths in date_paths.items():
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

        result.append(
            DateLoadTask(
                dateval=dateval,
                dataset_paths=dict(dataset_paths),
                chains_ready=date_chains_ready[dateval],
                chains_not_ready=date_chains_not_ready[dateval],
                write_manager=BQLoader(
                    location=write_to,
                    partition_cols=["dt"],
                    extra_marker_columns={},
                    extra_marker_columns_schema=[
                        pa.field("dt", pa.date32()),
                    ],
                    markers_table=MARKERS_TABLE,
                    expected_outputs=expected_outputs,
                    force=force_complete,
                ),
            )
        )

    log.info(f"Consolidated to {len(result)} dateval tasks.")
    return result
