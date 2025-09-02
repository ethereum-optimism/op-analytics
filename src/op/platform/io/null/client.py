from dataclasses import dataclass
from ....core.interfaces.product_io import ProductIO
from ....core.types.dataset import Dataset
from ....core.types.partition import Partition
from ....core.types.product_ref import ProductRef

@dataclass
class NullIO(ProductIO):
    """Placeholder IO: do not use at runtime. Your Dagster wrapper or ad-hoc
    script should replace with a real ProductIO (e.g., GcsParquetIO, LocalParquetIO)."""

    def write(self, product: ProductRef, data: Dataset, part: Partition) -> None:
        raise RuntimeError("NullIO cannot write; provide a concrete ProductIO at runtime.")

    def read(self, product: ProductRef, part: Partition) -> Dataset:
        raise RuntimeError("NullIO cannot read; provide a concrete ProductIO at runtime.")
