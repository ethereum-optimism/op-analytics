import polars as pl

from op.core.pipeline.pipeline_binding import PipelineBinding
from op.core.pipeline.node_binding_factory import make_node_binding
from op.core.types.product_ref import ProductRef
from op.platform.binders.polars import bind_polars_model
from op.platform.io.null.client import NullIO

# Reuse raw source bindings
from op.catalog.raw.dune.products import (
    FEES_SRC_BINDING,      # alias "txs"
    GAS_SRC_BINDING,       # alias "gas"
    GAS_FEES_SRC_BINDING,  # alias "fees"
)

__all__ = ["PIPE_POLARS", "AGG_POLARS"]

# Output product for this model
AGG_POLARS = ProductRef(domain=("conformed", "superchain"), name="agg_polars", version="v1")

# Build a node from a Python transform that uses Polars
node_rb = bind_polars_model(
    name="superchain_agg_polars",
    func_spec="op.catalog.models.agg_polars.transform:transform",   # your function returns pl.DataFrame
    inputs={
        "txs":  FEES_SRC_BINDING,
        "gas":  GAS_SRC_BINDING,
        "fees": GAS_FEES_SRC_BINDING,
    },
    output=AGG_POLARS,
    # infer output row type from package path: op.catalog.models.agg_polars.schema:Record
    output_row_spec="op.catalog.models.agg_polars.schema:Record",
    out_port="agg",
    dt_param_name="dt",
    # Optional shaping (helps avoid mixed-type inference issues):
    input_row_types={"txs": dict, "gas": dict, "fees": dict},
    input_required_fields={
        "txs":  ("dt","blockchain","num_txs","num_blocks","median_tx_fee_usd","tx_fee_usd","chain_id","display_name"),
        "gas":  ("dt","blockchain","sum_evm_gas_used","chain_id"),
        "fees": ("dt","blockchain","tx_fee_usd","chain_id"),
    },
    input_casts={
        "txs": {
            "dt": pl.Utf8, "blockchain": pl.Utf8, "display_name": pl.Utf8,
            "chain_id": pl.Int64, "num_txs": pl.Int64, "num_blocks": pl.Int64,
            "median_tx_fee_usd": pl.Float64, "tx_fee_usd": pl.Float64,
        },
        "gas": {
            "dt": pl.Utf8, "blockchain": pl.Utf8, "chain_id": pl.Int64,
            "sum_evm_gas_used": pl.Float64,
        },
        "fees": {
            "dt": pl.Utf8, "blockchain": pl.Utf8, "chain_id": pl.Int64,
            "tx_fee_usd": pl.Float64,
        },
    },
)

node_binding = make_node_binding(node_rb)

# Assemble a pipeline (sources + one transform node)
_NULL = NullIO()
PIPE_POLARS = PipelineBinding(
    sources=(FEES_SRC_BINDING, GAS_SRC_BINDING, GAS_FEES_SRC_BINDING),
    nodes=(node_binding,),
    io_for={
        FEES_SRC_BINDING.product: _NULL,
        GAS_SRC_BINDING.product: _NULL,
        GAS_FEES_SRC_BINDING.product: _NULL,
        AGG_POLARS: _NULL,
    },
)
