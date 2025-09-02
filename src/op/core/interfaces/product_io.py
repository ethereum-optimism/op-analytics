from typing import Protocol
from ..types.partition import Partition
from ..types.product_ref import ProductRef
from ..types.dataset import Dataset

class ProductIO(Protocol):
    """R/W for a product’s materialized data."""
    def read(self, product: ProductRef, part: Partition) -> Dataset: ...
    def write(self, product: ProductRef, ds: Dataset, part: Partition) -> str: ...
