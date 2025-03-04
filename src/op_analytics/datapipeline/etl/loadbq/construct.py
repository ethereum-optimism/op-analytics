from collections import defaultdict
from datetime import date

import pyarrow as pa

from op_analytics.coreutils.partitioned.dataaccess import init_data_access
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.coreutils.partitioned.reader import DataReader
from op_analytics.coreutils.partitioned.markers_core import DateFilter, MarkerFilter
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
    table_name_map: dict[str, str],
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

    # Prepare a request for output data to pre-fetch completion markers.
    # Markers are used to skip already completed tasks.
    output_root_paths = []
    for rp in root_paths_to_read:
        output_root_paths.append(
            BQOutputData.get_bq_root_path(
                root_path=rp.root_path,
                table_name_map=table_name_map,
                target_bq_dataset_name=bq_dataset_name,
            )
        )

    output_markers_df = query_output_markers(
        location=write_to,
        markers_table=markers_table,
        datevals=blockbatch_request.time_range.to_date_range().dates(),
        root_paths=output_root_paths,
    )
    log.info(f"pre-fetched {len(output_markers_df)} markers")

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
        complete_markers = []
        for root_path, parquet_paths in dataset_paths.items():
            bq_out = BQOutputData.construct(
                root_path=root_path,
                parquet_paths=parquet_paths,
                target_dateval=dateval,
                target_bq_dataset_name=bq_dataset_name,
                table_name_map=table_name_map,
            )
            expected_outputs.append(bq_out.expected_output)
            outputs.append(bq_out)

            is_complete = (
                bq_out.expected_output.marker_path in output_markers_df["marker_path"].to_list()
            )
            if is_complete:
                complete_markers.append(bq_out.expected_output.marker_path)

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
                    complete_markers=complete_markers,
                ),
                outputs=outputs,
            )
        )

    log.info(f"Consolidated to {len(result)} dateval tasks.")
    return result


def query_output_markers(
    location: DataLocation,
    markers_table: str,
    datevals: list[date],
    root_paths: list[str],
):
    """Query Clickhouse to find out which output markers have been written already."""
    client = init_data_access()

    return client.query_markers_with_filters(
        data_location=location,
        markers_table=markers_table,
        datefilter=DateFilter(
            min_date=None,
            max_date=None,
            datevals=datevals,
        ),
        projections=[
            "dt",
            "marker_path",
            "root_path",
        ],
        filters={
            "root_paths": MarkerFilter(
                column="root_path",
                values=root_paths,
            ),
        },
    )
