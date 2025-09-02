from dataclasses import dataclass
from datetime import datetime, date
from typing import Mapping, Optional, Tuple


# +++ change (new fields) +++
@dataclass(frozen=True)
class FileMarker:
    env: str
    product_id: str
    root_path: str
    data_path: str
    partition: Mapping[str, str]
    row_count: int
    byte_size: Optional[int] = None
    span_min: Optional[str] = None
    span_max: Optional[str] = None
    span_kind: Optional[str] = None
    num_parts: Optional[int] = None
    process_name: str = "unknown"
    writer_name: str = "unknown"
    updated_at: datetime = datetime.utcnow()
    run_id: str = ""
    fingerprint: str = ""
    upstream: Tuple[str, ...] = ()
    # NEW:
    storage_id: str = ""           # e.g., "gcs:dev:bronze", "fs:local:bronze", "bq:prod:analytics"
    location_kind: str = ""        # "gcs" | "fs" | "bigquery" | "clickhouse"

    @property
    def dt(self) -> Optional[date]:
        v = self.partition.get("dt")
        return date.fromisoformat(v) if v else None
