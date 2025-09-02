from typing import Any
from ..types.product_ref import ProductRef
from .node_contracts import NodeContracts


def node_contracts(node: Any) -> NodeContracts:
    nc = getattr(node, "__node_contracts__", None)
    if nc is None:
        raise AttributeError(f"{node.__class__.__name__} has no __node_contracts__ (apply @contracts)")
    return nc

def required_product(node: Any, index: int = 0) -> ProductRef:
    nc = node_contracts(node)
    return nc.requires[index].product

def provided_product(node: Any, index: int = 0) -> ProductRef:
    nc = node_contracts(node)
    return nc.provides[index].product
