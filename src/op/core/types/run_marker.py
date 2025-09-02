from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class RunMarker:
    env: str
    product_id: str
    partition_key: str              # canonical (e.g., "dt=2025-08-31|chain=op")
    run_id: str
    status: str                     # started|succeeded|failed|skipped
    started_at: datetime
    finished_at: Optional[datetime] = None
    error_msg: Optional[str] = None
    attempt: int = 1
    executor: str = "dagster"       # or adhoc/cli
