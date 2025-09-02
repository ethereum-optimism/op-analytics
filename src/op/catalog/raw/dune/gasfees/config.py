from dataclasses import dataclass
from datetime import date, timedelta
from typing import Dict, Any

@dataclass(frozen=True)
class Config:
    anchor_day: date
    trailing_days: int = 30
    ending_days: int = 0
    single_chain: str = "none"
    use_dummy: bool = False

    def start_date(self, anchor: date | None = None) -> date:
        a = self.anchor_day if anchor is None else anchor
        return a - timedelta(days=self.ending_days + self.trailing_days)

    def end_date(self, anchor: date | None = None) -> date:
        a = self.anchor_day if anchor is None else anchor
        return a - timedelta(days=self.ending_days)

    def to_query_params(self, anchor: date | None = None) -> Dict[str, Any]:
        return {
            "start_date": self.start_date(anchor).isoformat(),
            "end_date": self.end_date(anchor).isoformat(),
            "single_chain": self.single_chain,
        }
