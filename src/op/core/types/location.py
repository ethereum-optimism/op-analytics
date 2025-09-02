from dataclasses import dataclass
from typing import Literal, Union, Tuple

@dataclass(frozen=True)
class FileLocation:
    storage_id: str                 # e.g., "gcs:dev:bronze" or "fs:local:bronze"
    uri_template: str               # e.g., "gs://{gcs_bucket}/{root}/raw/{domain}/{name}/{version}/dt={dt}/"
    format: Literal["parquet","csv","json"] = "parquet"

@dataclass(frozen=True)
class TableLocation:
    storage_id: str                 # e.g., "bq:corp.analytics" or "ch:analytics"
    system: Literal["bigquery","clickhouse"]
    database: str                   # "project.dataset" (BQ) or "db" (CH)
    table_template: str             # "{domain}__{name}__{version}"
    partition_column: str | None = None

Location = Union[FileLocation, TableLocation]

@dataclass(frozen=True)
class MultiLocation:
    primary: Location
    legacy: Tuple[Location, ...] = ()
