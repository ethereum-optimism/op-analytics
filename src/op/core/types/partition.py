# from dataclasses import dataclass
# from typing import Mapping, Optional

# @dataclass(frozen=True)
# class Partition:
#     values: Mapping[str, str]
#     @staticmethod
#     def from_partition_key(key: Optional[str]) -> "Partition":
#         return Partition(values={"dt": key} if key else {})

from dataclasses import dataclass
from typing import Mapping

@dataclass(frozen=True)
class Partition:
    """Simple, typed-ish partition dict."""
    values: Mapping[str, str]

    def get(self, key: str, default: str | None = None) -> str | None:
        return self.values.get(key, default)

    def require(self, key: str) -> str:
        v = self.values.get(key)
        if v is None:
            raise ValueError(f"Partition missing required key '{key}'")
        return v

def want_date(part: Partition, key: str = "dt") -> str:
    """YYYY-MM-DD string; raises if absent."""
    v = part.require(key)
    # Add stricter validation if you like
    return v
