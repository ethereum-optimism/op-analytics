from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Mapping

Status = Literal["STARTED","SUCCEEDED","FAILED","SKIPPED"]

@dataclass(frozen=True)
class MarkerRecord:
    env: str
    product_id: str
    partition_key: str             # deterministic string (e.g., dt=2025-08-31|hour=13)
    run_id: str
    status: Status
    started_at: datetime
    finished_at: datetime | None
    output_uri: str | None
    rows: int | None
    fingerprint: str | None        # algo_id + upstream markers summary
    meta: Mapping[str, str] = None # free-form
