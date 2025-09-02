from dataclasses import dataclass
from typing import Generic, Type, TypeVar

T = TypeVar("T")
R = TypeVar("R")

@dataclass(frozen=True)
class ResourceRef(Generic[T]):
    """Pointer to a Dagster resource by key, with expected Python type for validation."""
    key: str
    expected: Type[T]
