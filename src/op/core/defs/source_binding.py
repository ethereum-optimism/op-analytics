from dataclasses import dataclass
from typing import Generic, TypeVar, Optional, Type

from .source_meta import SourceMeta
from ..types.product_ref import ProductRef
from ..interfaces.source import Source
from ..defs.resource_binding import ResourceBinding

T = TypeVar("T")


@dataclass(frozen=True)
class SourceBinding(Generic[T]):
    product: ProductRef[T]
    source: ResourceBinding[Source, Source]
    name: str
    row_type: Optional[Type] = None
    meta: Optional[SourceMeta] = None
