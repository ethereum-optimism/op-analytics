from typing import Protocol, Generic, TypeVar

InT = TypeVar("InT")
OutT = TypeVar("OutT")


class Node(Protocol, Generic[InT, OutT]):
    """Pure business logic: algorithm identity + execution."""
    def algo_id(self) -> str: ...
    def execute(self, input: InT, part) -> OutT: ...
