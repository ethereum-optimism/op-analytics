from abc import ABC, abstractmethod
from typing import Iterable, TypeVar

T = TypeVar("T")


class SqlRunner(ABC):
    @abstractmethod
    def run_sql(self, sql: str) -> Iterable[dict]: ...
