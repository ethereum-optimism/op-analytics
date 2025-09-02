from dataclasses import dataclass
from typing import Any, Dict, Generic, Type, TypeVar

from ..types.resource_ref import ResourceRef

T = TypeVar("T")
R = TypeVar("R")


@dataclass(frozen=True)
class ResourceBinding(Generic[T, R]):
    """Holds a concrete instance + stable key + expected type; can export to Definitions()."""
    key: str
    resource: R
    expected: Type[T]

    def ref(self) -> ResourceRef[T]:
        return ResourceRef[T](key=self.key, expected=self.expected)

    def export(self) -> Dict[str, Any]:
        return {self.key: self.resource}
