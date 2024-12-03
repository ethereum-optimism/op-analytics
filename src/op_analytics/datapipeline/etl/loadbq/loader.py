from dataclasses import dataclass
from datetime import date
from typing import Any

import pyarrow as pa

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.bigquery.load import load_from_parquet_uris
from op_analytics.coreutils.partitioned import (
    DataLocation,
    ExpectedOutput,
    OutputPartMeta,
    WriteManager,
    PartitionedRootPath,
    PartitionedMarkerPath,
)
from overrides import override


log = structlog.get_logger()


@dataclass
class BQOutputData:
    source_uris: list[str]
    source_uris_root_path: str
    dateval: date
    bq_dataset_name: str
    bq_table_name: str


class BQLoader(WriteManager):
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

        written_part = OutputPartMeta.from_tuples(
            partitions=[("dt", output_data.dateval.strftime("%Y-%m-%d"))],
            row_count=num_parquet,  # Not the actual row count, but the number of paths loaded.
        )

        log.info(f"DONE Loading {num_parquet} files to {bq_destination}")

        return [written_part]


def bq_load(
    location: DataLocation,
    dateval: date,
    bq_dataset_name: str,
    bq_table_name: str,
    markers_table: str,
    source_uris: list[str],
    source_uris_root_path: str,
    force_complete: bool,
) -> list[OutputPartMeta]:
    """Use a WriteManager class to handle writing completion markers."""
    location.ensure_biguqery()

    manager = BQLoader(
        location=location,
        expected_output=ExpectedOutput(
            dataset_name=bq_table_name,
            root_path=PartitionedRootPath(""),  # Not meaningful for BQ Load
            file_name="",  # Not meaningful for BQ Load
            marker_path=PartitionedMarkerPath(
                f"{bq_dataset_name}/{bq_table_name}/{dateval.strftime("%Y-%m-%d")}"
            ),
            process_name="default",
            additional_columns={},
            additional_columns_schema=[
                pa.field("dt", pa.date32()),
            ],
        ),
        markers_table=markers_table,
        force=force_complete,
    )

    return manager.write(
        output_data=BQOutputData(
            source_uris=source_uris,
            source_uris_root_path=source_uris_root_path,
            dateval=dateval,
            bq_dataset_name=bq_dataset_name,
            bq_table_name=bq_table_name,
        )
    )
