from typing import Protocol

from op_analytics.dataengine.core.types.product_ref import ProductRef


class Publishable(Protocol):
    """Capability for sinks to expose a ProductRef that describes their published output."""
    def product_ref(self) -> ProductRef:
        ...
