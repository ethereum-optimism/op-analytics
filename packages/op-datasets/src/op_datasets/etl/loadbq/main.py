from op_coreutils.bigquery.load import load_from_parquet_uris
from op_coreutils.logger import bind_contextvars, structlog
from op_coreutils.partitioned import (
    DataLocation,
    InputData,
    construct_inputs,
    get_dt,
    get_root_path,
)

from op_datasets.chains.chain_metadata import goldsky_chains

from .task import DateLoadTask, consolidate_chains

log = structlog.get_logger()


def load_superchain_raw_to_bq(
    range_spec: str,
    dryrun: bool,
    force: bool,
):
    # IMPORTANT: When loading to BigQuery we always load all the chains at once.
    # We do this because loading implies truncating any existing data in the date
    # partition.

    chains = goldsky_chains()
    inputs: list[InputData] = construct_inputs(
        chains=chains,
        range_spec=range_spec,
        read_from=DataLocation.GCS,
        input_datasets=["blocks", "transactions", "logs", "traces"],
    )

    if dryrun:
        log.info("DRYRUN: No work will be done.")
        return

    date_tasks = consolidate_chains(inputs)

    for i, task in enumerate(date_tasks):
        bind_contextvars(
            task=f"{i+1}/{len(inputs)}",
            **task.contextvars,
        )

        loader(task, force)


def loader(task: DateLoadTask, force: bool):
    if task.chains_not_ready:
        log.warning(f"INPUTS NOT READY: {sorted(task.chains_not_ready)}")
        if not force:
            return

    for dataset, parquet_paths in task.dataset_paths.items():
        root_path = get_root_path(parquet_paths)
        dateval = get_dt(parquet_paths)
        assert task.dateval == dateval

        load_from_parquet_uris(
            source_uris=parquet_paths,
            source_uri_prefix=root_path + "{chain:STRING}/{dt:DATE}",
            destination_dataset="superchain_raw",
            destination_table=dataset,
            date_partition=dateval,
            time_partition_field="dt",
            clustering_fields=["chain"],
        )
