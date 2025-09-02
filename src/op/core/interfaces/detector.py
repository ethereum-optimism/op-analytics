from typing import Generic, Protocol, TypeVar

from ..types.dataset import Dataset
from ..types.run_context import RunContext

T = TypeVar("T")


class Detector(Protocol, Generic[T]):
    """Detector that inspects a dataset and returns (passed, metrics)."""
    name: str
    def run(self, data: Dataset[T], ctx: RunContext) -> tuple[bool, dict]: ...
