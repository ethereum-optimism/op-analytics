from typing import Mapping

from op_analytics.dataengine.core.interfaces.dataset_catalog import DatasetCatalog
from op_analytics.dataengine.core.types.product_ref import ProductRef


class MemoryDatasetCatalog(DatasetCatalog):
    """In‑memory dataset catalog for tests and local runs."""
    def __init__(self) -> None:
        self._seen: dict[tuple[str, str], ProductRef] = {}

    def register(self, product: ProductRef, partition: Mapping[str, str], fingerprint: str) -> None:
        key = (product.name, product.version)
        self._seen[key] = product

    def resolve(self, product: ProductRef) -> ProductRef:
        key = (product.name, product.version)
        if key not in self._seen:
            raise KeyError(f"Unknown product: {product.name}:{product.version}")
        return self._seen[key]
