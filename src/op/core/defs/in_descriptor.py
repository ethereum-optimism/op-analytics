from dataclasses import dataclass
from op.core.types.product_ref import ProductRef

@dataclass(frozen=True)
class In:
    """Marker for transforms to declare the expected product for a parameter."""
    product: ProductRef
