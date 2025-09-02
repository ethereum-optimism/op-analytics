from op.core.pipeline.pipeline_binding import PipelineBinding
from op.platform.io.null.client import NullIO

# These SourceBindings are already defined alongside your raw Dune products
from op.catalog.raw.dune.products import (
    FEES_SRC_BINDING,
    GAS_SRC_BINDING,
    GAS_FEES_SRC_BINDING,
)

__all__ = ["PIPE_RAW_DUNE"]

_NULL = NullIO()

# No transform nodes; just make the three raw sources available as products.
PIPE_RAW_DUNE = PipelineBinding(
    sources=(FEES_SRC_BINDING, GAS_SRC_BINDING, GAS_FEES_SRC_BINDING),
    nodes=(),
    io_for={
        FEES_SRC_BINDING.product: _NULL,
        GAS_SRC_BINDING.product: _NULL,
        GAS_FEES_SRC_BINDING.product: _NULL,
    },
)
