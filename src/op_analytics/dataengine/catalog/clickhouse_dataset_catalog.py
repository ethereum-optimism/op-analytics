from typing import Mapping

from op_analytics.dataengine.core.interfaces.dataset_catalog import DatasetCatalog
from op_analytics.dataengine.core.types.product_ref import ProductRef


class ClickHouseDatasetCatalog(DatasetCatalog):
    """Catalog backed by ClickHouse (stub). Store product refs and resolve them for consumers."""
    def register(self, product: ProductRef, partition: Mapping[str, str], fingerprint: str) -> None:
        raise NotImplementedError
    def resolve(self, product: ProductRef) -> ProductRef:
        raise NotImplementedError
