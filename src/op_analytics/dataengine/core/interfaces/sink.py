from typing import Generic, Protocol, TypeVar, runtime_checkable

from op_analytics.dataengine.core.types.dataset import Dataset
from op_analytics.dataengine.core.types.run_context import RunContext

T = TypeVar("T")

@runtime_checkable
class Sink(Protocol, Generic[T]):
    """Side‑effecting sink that commits a Dataset[T] (returns None)."""
    def commit(self, data: Dataset[T], ctx: RunContext) -> None:
        ...
