from op_analytics.coreutils.logger import (
    bind_contextvars,
    bound_contextvars,
    structlog,
)
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.coreutils.partitioned.paths import get_dt, get_root_path
from op_analytics.coreutils.partitioned.reader import DataReader
from op_analytics.datapipeline.chains.goldsky_chains import goldsky_mainnet_chains
from op_analytics.datapipeline.etl.ingestion.markers import (
    INGESTION_MARKERS_TABLE,
)
from op_analytics.datapipeline.etl.ingestion.reader_bydate import construct_readers_bydate

from .loader import bq_load
from .task import consolidate_chains

log = structlog.get_logger()


MARKERS_TABLE = "superchain_raw_bigquery_markers"

BQ_PUBLIC_DATASET = "superchain_raw"

INGESTION_ROOT_PATHS = [
    "ingestion/blocks_v1",
    "ingestion/transactions_v1",
    "ingestion/logs_v1",
    "ingestion/traces_v1",
]


@bound_contextvars(pipeline_step="load_superchain_raw_to_bq")
def load_superchain_raw_to_bq(
    location: DataLocation,
    range_spec: str,
    dryrun: bool,
    force_complete: bool,
    force_not_ready: bool,
):
    # IMPORTANT: When loading to BigQuery we always load all the chains at once.
    # We do this because loading implies truncating any existing data in the date
    # partition.
    location.ensure_biguqery()

    chains = goldsky_mainnet_chains()
    inputs: list[DataReader] = construct_readers_bydate(
        chains=chains,
        range_spec=range_spec,
        read_from=DataLocation.GCS,
        markers_table=INGESTION_MARKERS_TABLE,
        root_paths=INGESTION_ROOT_PATHS,
    )

    if dryrun:
        log.info("DRYRUN: No work will be done.")
        return

    date_tasks = consolidate_chains(inputs)

    success = 0
    for i, task in enumerate(date_tasks):
        bind_contextvars(
            task=f"{i+1}/{len(date_tasks)}",
            **task.contextvars,
        )

        if task.chains_not_ready:
            log.warning("task", status="input_not_ready")
            log.warning(f"some chains are not ready to load to bq: {sorted(task.chains_not_ready)}")

        if task.chains_not_ready and not force_not_ready:
            continue

        for root_path, parquet_paths in task.dataset_paths.items():
            # Get the common root path for all the source parquet paths.
            source_uris_root_path = get_root_path(parquet_paths)
            assert source_uris_root_path.endswith(root_path + "/")

            # Get the common date partition for all the source parquet paths
            # and make sure it agrees with the task.
            dateval = get_dt(parquet_paths)
            assert task.dateval == dateval

            # Compute the BQ table name from the root path. For example:
            # root_path = "ingestion/traces_v1"  --> traces
            bq_table_name = root_path.split("/")[-1].split("_")[0]

            write_result = bq_load(
                location=location,
                dateval=dateval,
                bq_dataset_name=BQ_PUBLIC_DATASET,
                bq_table_name=bq_table_name,
                markers_table=MARKERS_TABLE,
                source_uris=parquet_paths,
                source_uris_root_path=source_uris_root_path,
                force_complete=force_complete,
            )
            log.info("task", status=write_result.status)
            success += 1
