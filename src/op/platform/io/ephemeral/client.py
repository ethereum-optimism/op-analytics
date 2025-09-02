from dataclasses import dataclass, field
from typing import Dict, Tuple

from ....core.defs.source_binding import SourceBinding
from ....core.interfaces.product_io import ProductIO
from ....core.types.dataset import Dataset
from ....core.types.partition import Partition
from ....core.types.product_ref import ProductRef

@dataclass
class EphemeralIO(ProductIO):
    """Volatile store keyed by (product_id, partition_key). Discarded after run."""
    _mem: Dict[Tuple[str, str], Dataset] = field(default_factory=dict)

    def _k(self, product: ProductRef, part: Partition) -> Tuple[str, str]:
        return (product.id, part.values.get("dt", "unpartitioned"))

    def write(self, product: ProductRef, data: Dataset, part: Partition) -> None:
        self._mem[self._k(product, part)] = data

    def read(self, product: ProductRef, part: Partition) -> Dataset:
        k = self._k(product, part)
        if k not in self._mem:
            raise KeyError(f"Ephemeral product not present in this run: {product.id} @ {part.values}")
        return self._mem[k]
