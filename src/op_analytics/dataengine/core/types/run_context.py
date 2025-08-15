from dataclasses import dataclass
from .partition import Partition

@dataclass(frozen=True)
class RunContext:
    """Per‑step context including pipeline/step identifiers and the active partition."""
    pipeline: str
    step_id: str
    partition: Partition
