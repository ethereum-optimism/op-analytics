from dataclasses import dataclass
from datetime import date
from typing import Any, Dict

@dataclass(frozen=True)
class Request:
    anchor_day: date | None = None
    chain: str = "all"     # example param you might want later

    def to_query_params(self, anchor: date | None = None) -> Dict[str, Any]:
        # The /protocols endpoint typically takes no params; include anchors if you need
        return {}
