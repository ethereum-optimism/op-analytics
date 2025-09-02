from dataclasses import dataclass, field
from typing import Generic, Sequence, TypeVar, Mapping, Any

T = TypeVar("T")

@dataclass
class Dataset(Generic[T]):
    """Typed dataset wrapper."""
    rows: Sequence[T]
    metadata: Mapping[str, Any] = field(default_factory=dict)
