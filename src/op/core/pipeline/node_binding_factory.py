from typing import Dict, TypeVar, Any, cast, Iterable, Tuple

from ..defs.node_binding import NodeBinding
from ..defs.node_contracts import NodeContracts
from ..defs.ports import Port
from ..defs.product_contract import ProductContract
from ..defs.source_binding import SourceBinding
from ..interfaces.node import Node
from ..types.dataset import Dataset
from ..types.product_ref import ProductRef
from ..utils.helpers import field_names

N = TypeVar("N", bound=Node[Dict[ProductRef, Dataset], Dict[ProductRef, Dataset]])

def _extract_contracts(node: Any) -> NodeContracts:
    # 1) Legacy attribute
    nc = getattr(node, "__node_contracts__", None)
    if isinstance(nc, NodeContracts):
        return nc

    # 2) Property-style attribute
    maybe = getattr(node, "contracts", None)
    if isinstance(maybe, NodeContracts):
        return maybe
    if callable(maybe):
        val = maybe()  # support legacy method form
        if isinstance(val, NodeContracts):
            return val

    raise ValueError(
        f"Node {node.__class__.__name__} has no contracts. "
        "Attach via @contracts(...), set a `contracts` property, "
        "or provide `__node_contracts__`."
    )

def _normalize_to_product_contracts(
    seq: Iterable[Any],
    role: str,
) -> Tuple[ProductContract, ...]:
    """
    Accept either:
      - ProductContract
      - Port(name, contract=ProductContract)
    Reject:
      - SourceBinding (helpful message)
      - any other type
    """
    out: list[ProductContract] = []
    for i, item in enumerate(seq):
        if isinstance(item, ProductContract):
            out.append(item)
            continue
        if isinstance(item, Port):
            if not isinstance(item.contract, ProductContract):
                raise TypeError(
                    f"Node contracts.{role}[{i}] Port has non-ProductContract: {type(item.contract).__name__}"
                )
            out.append(item.contract)
            continue
        if isinstance(item, SourceBinding):
            raise TypeError(
                f"Node contracts.{role}[{i}] is a SourceBinding, but expected a ProductContract. "
                f"Build a ProductContract with product={item.product}, row_type=..., required_fields=..."
            )
        raise TypeError(
            f"Node contracts.{role}[{i}] is {type(item).__name__}, expected ProductContract or Port."
        )
    return tuple(out)

def _validate_product_contracts(pcs: Tuple[ProductContract, ...], role: str) -> Tuple[ProductContract, ...]:
    # Minimal structural check: row types are introspectable
    for i, pc in enumerate(pcs):
        _ = field_names(pc.row_type)  # ensure dataclass/dict-friendly shape
        # (optional) validate required_fields subset here if you want stricter checks
    return pcs

def make_node_binding(node_rb: Any) -> NodeBinding:
    """
    Create a NodeBinding from a ResourceBinding[Node], by introspecting contracts
    attached to the node instance. Keeps NodeBinding free of infra concerns.
    """
    node = node_rb.resource  # ResourceBinding.key/resource/expected
    nc = _extract_contracts(node)

    req_pcs = _normalize_to_product_contracts(nc.requires, role="requires")
    prv_pcs = _normalize_to_product_contracts(nc.provides, role="provides")

    req_pcs = _validate_product_contracts(req_pcs, "requires")
    prv_pcs = _validate_product_contracts(prv_pcs, "provides")

    return NodeBinding(
        node=node,
        requires=req_pcs,
        provides=prv_pcs,
        config=None,
    )
