from dataclasses import dataclass
from typing import Generic, TypeVar
import polars as pl

from ..types.product_ref import ProductRef
from ..defs.source_meta import SourceMeta

T = TypeVar("T")  # row type

@dataclass(frozen=True)
class Frame(Generic[T]):
    """A Polars DataFrame plus strong typing and provenance."""
    df: pl.DataFrame
    product: ProductRef[T]
    meta: SourceMeta
