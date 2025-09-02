from dataclasses import dataclass
from datetime import date
from typing import Any, Dict

@dataclass(frozen=True)
class Config:
    """
    Request config for the 'gas used by chain per day' query.

    Note: anchor_day is accepted for compatibility with our SqlSource constructor,
    but this template uses NOW() and does not require the anchor explicitly.
    """
    anchor_day: date
    trailing_days: int = 30          # how many days to include (lookback)
    ending_days: int = 0             # 0 = include up to 'today'; >0 shifts the end back
    single_chain: str = "none"       # 'none' = all chains; otherwise filter by name

    def to_query_params(self, anchor: date | None = None) -> Dict[str, Any]:
        # The SQL template needs only these three params.
        return {
            "trailing_days": self.trailing_days,
            "ending_days": self.ending_days,
            "single_chain": self.single_chain,
        }
