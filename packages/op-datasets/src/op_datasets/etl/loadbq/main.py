from dataclasses import dataclass
from datetime import date
from typing import Any

import pyarrow as pa
from op_coreutils.bigquery.load import load_from_parquet_uris
from op_coreutils.logger import bind_contextvars, clear_contextvars, structlog
from op_coreutils.partitioned import (
    DataLocation,
    DataReader,
    ExpectedOutput,
    KeyValue,
    OutputPartMeta,
    SinkMarkerPath,
    SinkOutputRootPath,
    WriteManager,
    construct_input_batches,
    get_dt,
    get_root_path,
)
from overrides import override

from op_datasets.chains.goldsky_chains import goldsky_mainnet_chains
from op_datasets.etl.ingestion.markers import INGESTION_DATASETS, INGESTION_MARKERS_TABLE

from .task import consolidate_chains

log = structlog.get_logger()


MARKERS_TABLE = "superchain_raw_bigquery_markers"

BQ_PUBLIC_DATASET = "superchain_raw"


def load_superchain_raw_to_bq(
    range_spec: str,
    dryrun: bool,
    force: bool,
):
    # IMPORTANT: When loading to BigQuery we always load all the chains at once.
    # We do this because loading implies truncating any existing data in the date
    # partition.

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

    for i, task in enumerate(date_tasks):
        clear_contextvars()
        bind_contextvars(
            task=f"{i+1}/{len(date_tasks)}",
            **task.contextvars,
        )

        if task.chains_not_ready:
            log.warning(f"INPUTS NOT READY: {sorted(task.chains_not_ready)}")
            if not force:
                return

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

            # Set up manager to handle completion markers.
            manager = BQLoadManager(
                location=DataLocation.BIGQUERY,
                expected_output=ExpectedOutput(
                    dataset_name=bq_table_name,
                    root_path=SinkOutputRootPath(""),  # Not meaningful for BQ Load
                    file_name="",  # Not meaningful for BQ Load
                    marker_path=SinkMarkerPath(
                        f"{BQ_PUBLIC_DATASET}/{bq_table_name}/{dateval.strftime("%Y-%m-%d")}"
                    ),
                    process_name="default",
                    additional_columns={},
                    additional_columns_schema=[
                        pa.field("dt", pa.date32()),
                    ],
                ),
                markers_table=MARKERS_TABLE,
                force=force,
            )

            manager.write(
                output_data=BQOutputData(
                    source_uris=parquet_paths,
                    source_uris_root_path=source_uris_root_path,
                    dateval=dateval,
                    bq_dataset_name=BQ_PUBLIC_DATASET,
                    bq_table_name=bq_table_name,
                )
            )


@dataclass
class BQOutputData:
    source_uris: list[str]
    source_uris_root_path: str
    dateval: date
    bq_dataset_name: str
    bq_table_name: str


class BQLoadManager(WriteManager):
    @override
    def write_implementation(self, output_data: Any) -> list[OutputPartMeta]:
        assert isinstance(output_data, BQOutputData)

        num_parquet = len(output_data.source_uris)
        bq_destination = f"{output_data.bq_dataset_name}.{output_data.bq_table_name}"
        log.info(f"BEGIN Loading {num_parquet} files to {bq_destination}")

        load_from_parquet_uris(
            source_uris=output_data.source_uris,
            source_uri_prefix=output_data.source_uris_root_path + "{chain:STRING}/{dt:DATE}",
            destination=bq_destination,
            date_partition=output_data.dateval,
            time_partition_field="dt",
            clustering_fields=["chain"],
        )

        written_part = OutputPartMeta(
            partitions=[KeyValue(key="dt", value=output_data.dateval.strftime("%Y-%m-%d"))],
            row_count=num_parquet,  # Not the actual row count, but the number of paths loaded.
        )

        log.info(f"DONE Loading {num_parquet} files to {bq_destination}")

        return [written_part]
