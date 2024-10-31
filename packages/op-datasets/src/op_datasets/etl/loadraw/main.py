from op_coreutils.bigquery.load import load_from_parquet_uris
from op_coreutils.logger import bind_contextvars, structlog
from op_coreutils.partitioned.location import DataLocation
from op_coreutils.time import date_fromstr

from op_datasets.etl.intermediate.construct import construct_tasks
from op_datasets.etl.intermediate.task import IntermediateModelsTask

log = structlog.get_logger()


def load_superchain_raw_to_bq(
    chains: list[str],
    range_spec: str,
    dryrun: bool,
):
    tasks = construct_tasks(
        chains=chains,
        models=[],
        range_spec=range_spec,
        read_from=DataLocation.GCS,
        write_to=[],
    )

    if dryrun:
        log.info("DRYRUN: No work will be done.")
        return

    for i, task in enumerate(tasks):
        bind_contextvars(
            task=f"{i+1}/{len(tasks)}",
            **task.contextvars,
        )

        if not task.inputs_ready:
            log.warning("Task inputs are not ready. Skipping this task.")
            continue

        loader(task)


def loader(task: IntermediateModelsTask):
    for dataset, parquet_paths in task.dataset_paths.items():
        # Use the first path to determine the prefix.
        root_path = parquet_paths[0].split("chain=")[0]
        assert all(_.startswith(root_path) for _ in parquet_paths)

        # Determine the date partition that we need to replace.
        dates = {_.split("/dt=")[-1].split("/")[0] for _ in parquet_paths}
        assert len(dates) == 1
        dateval = date_fromstr(list(dates)[0])

        load_from_parquet_uris(
            source_uris=parquet_paths,
            source_uri_prefix=root_path + "{chain:STRING}/{dt:DATE}",
            destination_dataset="superchain_raw",
            destination_table=dataset,
            date_partition=dateval,
        )
        pass
