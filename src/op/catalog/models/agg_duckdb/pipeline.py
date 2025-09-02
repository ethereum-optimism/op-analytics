# from ...curated.dune.pipeline import PIPE_CURATED_TXS
from ....core.pipeline.pipeline_binding import PipelineBinding
from ....core.pipeline.node_binding_factory import make_node_binding
from ....core.types.product_ref import ProductRef
from ....platform.binders.duckdb import bind_duckdb_model
from ....platform.io.null.client import NullIO

# Reuse raw source bindings
from op.catalog.raw.dune.products import (
    FEES_SRC_BINDING,      # alias "txs"   (daily txs/fees/latency by chain)
    GAS_SRC_BINDING,       # alias "gas"   (sum EVM gas used)
    GAS_FEES_SRC_BINDING,  # alias "fees"  (daily fee $ by chain)
)

__all__ = ["PIPE_DUCKDB", "AGG_DUCKDB"]

# Output product for this model
AGG_DUCKDB = ProductRef(domain=("conformed", "superchain"), name="agg_duckdb", version="v1")

# Build a node from the DuckDB SQL template package
node_rb = bind_duckdb_model(
    name="superchain_agg_duckdb",
    template_pkg="op.catalog.models.agg_duckdb",   # expects templates/query.sql.j2 & schema:Record
    inputs={
        "txs":  FEES_SRC_BINDING,
        "gas":  GAS_SRC_BINDING,
        "fees": GAS_FEES_SRC_BINDING,
    },
    output=AGG_DUCKDB,
    input_required_fields={
        "txs":  ("dt","blockchain","num_txs","num_blocks","median_tx_fee_usd","tx_fee_usd","chain_id","display_name"),
        "gas":  ("dt","blockchain","sum_evm_gas_used","chain_id"),
        "fees": ("dt","blockchain","tx_fee_usd","chain_id"),
    },
    out_port="agg",
    dt_param_name="dt",
)

node_binding = make_node_binding(node_rb)

# Assemble a pipeline (sources + one transform node)
_NULL = NullIO()
PIPE_DUCKDB = PipelineBinding(
    sources=(FEES_SRC_BINDING, GAS_SRC_BINDING, GAS_FEES_SRC_BINDING),
    nodes=(node_binding,),
    io_for={
        FEES_SRC_BINDING.product: _NULL,
        GAS_SRC_BINDING.product: _NULL,
        GAS_FEES_SRC_BINDING.product: _NULL,
        AGG_DUCKDB: _NULL,
    },
)
