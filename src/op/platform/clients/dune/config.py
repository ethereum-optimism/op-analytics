from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Config:
    """Config for DuneClient."""
    api_key: Optional[str] = None
    timeout_seconds: int = 300
    poll_interval_seconds: int = 5
    max_retries: int = 3
    performance: str = "medium"  # spice: "small" | "medium" | "large"
