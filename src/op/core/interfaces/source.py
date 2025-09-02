from abc import ABC, abstractmethod
from typing import Generic, Iterable, TypeVar
from ..types.partition import Partition

T = TypeVar("T")


class Source(Generic[T], ABC):
    """Abstract source yielding typed rows for a given partition."""
    @abstractmethod
    def fetch(self, partition: Partition) -> Iterable[T]: ...
