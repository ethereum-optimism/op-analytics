from dataclasses import dataclass
from typing import Dict

from op.core.interfaces.node import Node
from op.core.types.dataset import Dataset
from op.core.types.partition import Partition
from op.core.types.product_ref import ProductRef

@dataclass(frozen=True)
class IdentityNode(Node[Dict[ProductRef, Dataset], Dict[ProductRef, Dataset]]):
    inp: ProductRef
    out: ProductRef

    def execute(self, input: Dict[ProductRef, Dataset], part: Partition) -> Dict[ProductRef, Dataset]:
        return {self.out: input[self.inp]}
