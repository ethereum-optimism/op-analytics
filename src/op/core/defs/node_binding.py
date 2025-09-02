from dataclasses import dataclass
from typing import Dict

from .product_contract import ProductContract
from ..interfaces.node import Node
from ..types.dataset import Dataset
from ..types.product_ref import ProductRef
from ..utils.helpers import field_names


@dataclass(frozen=True)
class NodeBinding:
    """Binds a node to explicit product/type contracts and validates wiring."""
    node: Node[Dict[ProductRef, Dataset], Dict[ProductRef, Dataset]]
    requires: tuple[ProductContract, ...]
    provides: tuple[ProductContract, ...]
    # optional: extra shape constraints
    config: dict | None = None

    def __post_init__(self):
        # Validate declared required/provided types have needed fields
        for req in self.requires:
            _ = field_names(req.row_type)  # ensures type is introspectable

        for prov in self.provides:
            _ = field_names(prov.row_type)

        # Attach lineage/type hints for tooling
        object.__setattr__(self.node, "__requires__", tuple(rc.product for rc in self.requires))
        object.__setattr__(self.node, "__provides__", tuple(pc.product for pc in self.provides))
        object.__setattr__(self.node, "__requires_types__", {rc.product: rc.row_type for rc in self.requires})
        object.__setattr__(self.node, "__provides_types__", {pc.product: pc.row_type for pc in self.provides})
