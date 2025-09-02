from dataclasses import dataclass
from typing import Literal, Optional, Mapping, Any

Kind = Literal["sql", "http", "rpc", "file"]

@dataclass(frozen=True)
class SourceMeta:
    kind: Kind                         # "sql" | "http" | "rpc" | "file"
    provider: str                      # e.g. "dune", "defillama", "clickhouse"
    name: str                          # e.g. "fees", "protocols"
    details: Mapping[str, Any]         # e.g. {"base_url": "...", "path": "..."} or {"templates_dir": "..."}
    runner: Optional[str] = None       # e.g. "DuneClient", "BasicHttpRunner"
