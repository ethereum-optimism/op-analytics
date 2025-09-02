from typing import Set, Dict

from .node_binding_factory import make_node_binding
from .pipeline_binding import PipelineBinding
from ..defs.node_binding import NodeBinding
from ..defs.resource_binding import ResourceBinding
from ..interfaces.node import Node
from ..interfaces.product_io import ProductIO
from ..types.product_ref import ProductRef
from ...platform.configs.bindings import PipelineConfig


def bind_node(node: Node) -> NodeBinding:
    """Introspect contracts on the node (via @contracts or .contracts()) and build a NodeBinding."""
    rb = ResourceBinding(key=f"node_{node.__class__.__name__}", resource=node, expected=Node)
    return make_node_binding(rb)


def build_pipeline(cfg: PipelineConfig) -> PipelineBinding:
    cfg.validate()

    # Discover all produced products (sources + node.provides)
    produced: Set[ProductRef] = set(s.product for s in cfg.sources)
    for nb in cfg.nodes:
        for pc in nb.provides:
            produced.add(pc.product)

    # Fill IO map: default for everything, allow overrides per-product
    io_for: Dict[ProductRef, ProductIO] = {p: cfg.default_io for p in produced}
    io_for.update(cfg.io_overrides)

    return PipelineBinding(
        sources=tuple(cfg.sources),
        nodes=tuple(cfg.nodes),
        io_for=io_for,
    )
