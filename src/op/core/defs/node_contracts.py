from dataclasses import dataclass
from typing import Tuple
from .ports import Port


@dataclass(frozen=True)
class NodeContracts:
    # Normalized to ports with names
    requires: Tuple[Port, ...]
    provides: Tuple[Port, ...]
