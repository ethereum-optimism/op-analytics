from op_analytics.coreutils.logger import (
    bind_contextvars,
    bound_contextvars,
    structlog,
)
from op_analytics.coreutils.partitioned import (
    DataLocation,
    DataReader,
    construct_input_batches,
    get_dt,
    get_root_path,
)
from op_analytics.datapipeline.chains.goldsky_chains import goldsky_mainnet_chains
from op_analytics.datapipeline.etl.ingestion.markers import (
    INGESTION_DATASETS,
    INGESTION_MARKERS_TABLE,
)

from .loader import bq_load
from .task import consolidate_chains

log = structlog.get_logger()


MARKERS_TABLE = "superchain_raw_bigquery_markers"

BQ_PUBLIC_DATASET = "superchain_raw"


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
    inputs: list[DataReader] = construct_input_batches(
        chains=chains,
        range_spec=range_spec,
        read_from=DataLocation.GCS,
        markers_table=INGESTION_MARKERS_TABLE,
        dataset_names=INGESTION_DATASETS,
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

        for dataset, parquet_paths in task.dataset_paths.items():
            # Get the common root path for all the source parquet paths.
            source_uris_root_path = get_root_path(parquet_paths)

            # Get the common date partition for all the source parquet paths
            # and make sure it agrees with the task.
            dateval = get_dt(parquet_paths)
            assert task.dateval == dateval

            # There is a name collision between what we call a dataset and what BigQuery
            # calls a dataset. For us a dataset is the name we use to refer to the output
            # data. In BQ a dataset is a collection of tables.
            bq_table_name = dataset

            bq_load(
                location=location,
                dateval=dateval,
                bq_dataset_name=BQ_PUBLIC_DATASET,
                bq_table_name=bq_table_name,
                markers_table=MARKERS_TABLE,
                source_uris=parquet_paths,
                source_uris_root_path=source_uris_root_path,
                force_complete=force_complete,
            )
            log.info("task", status="success")
            success += 1
