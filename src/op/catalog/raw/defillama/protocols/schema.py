from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any


@dataclass(frozen=True)
class Record:
    # From the posted schema (with safe defaults so missing keys don't break)
    id: Optional[str] = None
    name: Optional[str] = None
    symbol: Optional[str] = None
    category: Optional[str] = None
    chains: List[str] = field(default_factory=list)
    tvl: Optional[float] = None
    chainTvls: Dict[str, float] = field(default_factory=dict)
    change_1d: Optional[float] = None
    change_7d: Optional[float] = None
    extras: Dict[str, Any] = field(default_factory=dict)
