from dataclasses import dataclass
from typing import Mapping

@dataclass(frozen=True)
class Partition:
    """Structured partition context (e.g., {"dt": "2025-08-12", "chain": "eth"})."""
    values: Mapping[str, str]
