from typing import Mapping, Protocol

from op_analytics.dataengine.core.types.product_ref import ProductRef


class DatasetCatalog(Protocol):
    """Registry that records and resolves published datasets for downstream consumption."""
    def register(self, product: ProductRef, partition: Mapping[str, str], fingerprint: str) -> None:
        ...
    def resolve(self, product: ProductRef) -> ProductRef:
        """Return a normalized/validated ProductRef (or raise if unknown)."""
        ...
