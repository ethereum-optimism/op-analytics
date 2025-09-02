import re
from typing import Iterable

from ...core.types.product_ref import ProductRef

_ID = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def source_key(product: ProductRef, flavor: str) -> str:
    # Unique, deterministic; safe across modules (e.g., "src::raw/dune/fees/v1::sql")
    return f"src::{product.id}::{flavor}"


def io_key(layer: str) -> str:
    # If you want consistent io_manager names like "io::raw", "io::curated"
    return f"io::{layer}"


def make_key(*parts: Iterable[str]) -> str:
    """Join parts into a python identifier suitable for a Dagster resource key."""
    raw = "_".join(str(p) for ps in parts for p in (ps if isinstance(ps, (list, tuple)) else [ps]))
    key = "".join(c if c.isalnum() or c == "_" else "_" for c in raw)
    if not key or not _ID.match(key):  # ensure valid identifier
        key = f"r_{key}" if key else "r_default"
    return key
