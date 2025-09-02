from ....core.pipeline.pipeline_binding import PipelineBinding
from ....platform.io.null.client import NullIO

# raw Dune sources
from ....catalog.raw.dune.products import FEES_SRC_BINDING, GAS_SRC_BINDING, GAS_FEES_SRC_BINDING
# curated polars nodes
from ....catalog.curated.dune.products import TXS_CURATED_NODE, TXS_CURATED


__all__ = ["PIPE_CURATED_TXS", "TXS_CURATED"]

_NULL = NullIO()

PIPE_CURATED_TXS = PipelineBinding(
    sources=(),
    nodes=TXS_CURATED_NODE,
    io_for={
        FEES_SRC_BINDING.product: _NULL,
        TXS_CURATED: _NULL,
    },
)
