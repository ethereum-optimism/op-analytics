from dataclasses import dataclass
from datetime import date


from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.paths import get_dt, get_root_path

from .loader import BQLoader, BQOutputData


log = structlog.get_logger()

MARKERS_TABLE = "superchain_raw_bigquery_markers"


@dataclass
class DateLoadTask:
    """Task to load all data for a given date to BigQuery."""

    dateval: date
    dataset_paths: dict[str, list[str]]
    chains_ready: set[str]
    chains_not_ready: set[str]
    write_manager: BQLoader

    @property
    def contextvars(self):
        return {"date": self.dateval.strftime("%Y-%m-%d")}

    def output_tables(self, bq_dataset: str) -> list[BQOutputData]:
        tables: list[BQOutputData] = []

        for root_path, parquet_paths in self.dataset_paths.items():
            # Get the common root path for all the source parquet paths.
            source_uris_root_path = get_root_path(parquet_paths)
            assert source_uris_root_path.endswith(root_path + "/")

            # Get the common date partition for all the source parquet paths
            # and make sure it agrees with the task.
            dateval = get_dt(parquet_paths)
            assert self.dateval == dateval

            # Compute the BQ table name from the root path. For example:
            # root_path = "ingestion/traces_v1"  --> traces
            bq_table_name = root_path.split("/")[-1].split("_")[0]

            self.write_manager.location.ensure_biguqery()

            bq_root_path = f"{bq_dataset}/{bq_table_name}"

            tables.append(
                BQOutputData(
                    root_path=bq_root_path,
                    source_uris=parquet_paths,
                    source_uris_root_path=source_uris_root_path,
                    dateval=dateval,
                    bq_dataset_name=bq_dataset,
                    bq_table_name=bq_table_name,
                )
            )

        return tables
