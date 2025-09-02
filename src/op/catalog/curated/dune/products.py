from ....core.pipeline.node_binding_factory import make_node_binding
from ....core.types.product_ref import ProductRef
from ....platform.binders.polars import bind_polars_model
from ....catalog.raw.dune.products import FEES_SRC_BINDING  # source alias "txs"

TXS_CURATED = ProductRef(domain=("ephemeral","dune"), name="txs_curated", version="v1")

NODE_RB = bind_polars_model(
    name="txs_curated_polars",
    func_spec="op.catalog.curated.dune.fees.transform:transform",
    inputs={"txs": FEES_SRC_BINDING},
    output=TXS_CURATED,
    output_row_spec="op.catalog.curated.dune.fees.schema:Record",
    out_port="out",
    dt_param_name="dt",
    input_row_types={"txs": dict},
    input_required_fields={
        "txs": ("dt","blockchain","num_txs","num_blocks","median_tx_fee_usd","tx_fee_usd"),
    },
)
TXS_CURATED_NODE = make_node_binding(NODE_RB)
