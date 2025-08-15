from dataclasses import dataclass
from typing import Generic, List, TypeVar

T = TypeVar("T")

@dataclass
class Dataset(Generic[T]):
    """A typed collection of rows with schema metadata flowing between steps."""
    rows: List[T]
    schema: str
    schema_version: str
