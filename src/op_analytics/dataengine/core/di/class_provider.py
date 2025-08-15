from dataclasses import dataclass
from typing import Any, Dict, Generic, Type, TypeVar

T = TypeVar("T")

@dataclass
class ClassProvider(Generic[T]):
    """Provides instances by calling a registered class with default kwargs."""
    cls: Type[T]
    kwargs: Dict[str, Any]
    def create(self) -> T:
        return self.cls(**self.kwargs)
