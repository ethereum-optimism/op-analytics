from dataclasses import dataclass
from datetime import date

from op.core.types.env import EnvProfile
from op.core.types.location import FileLocation
from op.core.types.partition import Partition
from op.core.types.policies import PipelineRunConfig, SkipPolicy, UpstreamPolicy
from op.core.types.product_ref import ProductRef
from op.core.types.registry import ProductRegistry, ProductRegistration
from op.platform.io.memory.client import Client

from op.platform.markers.memory_store import MemoryMarkerStore

from op.core.pipeline.pipeline_binding import PipelineBinding
from op.core.defs.product_contract import ProductContract

from op.catalog.raw.dune.products import FEES_SRC_BINDING, GAS_SRC_BINDING, GAS_FEES_SRC_BINDING
from op.platform.binders.polars import bind_polars_model


@dataclass(frozen=True)
class AggRow:
    dt: str
    chain_id: int
    display_name: str
    num_txs: int
    num_blocks: int
    median_tx_fee_usd: float
    tx_fee_usd: float
    sum_evm_gas_used: float


def main() -> None:
    ENV = EnvProfile("test", vars={"root_prefix": "mem"})
    REG = ProductRegistry(_entries={})
    MARKERS = MemoryMarkerStore()
    IO = Client(ENV, REG, MARKERS)

    AGG = ProductRef(domain=("curated","dune"), name="superchain_agg", version="v1")
    for sb in (FEES_SRC_BINDING, GAS_SRC_BINDING, GAS_FEES_SRC_BINDING):
        REG.register(ProductRegistration(
            product=sb.product,
            by_env={"test": FileLocation(
                storage_id="mem:test:bronze",
                uri_template="memory:///{root_prefix}/{domain}/{name}/{version}/dt={dt}/",
                format="parquet",
            )}
        ))
    REG.register(ProductRegistration(
        product=AGG,
        by_env={"test": FileLocation(
            storage_id="mem:test:silver",
            uri_template="memory:///{root_prefix}/{domain}/{name}/{version}/dt={dt}/",
            format="parquet",
        )}
    ))

    cfg = PolarsModelConfig(
        name="superchain_agg_polars",
        func_spec="op.catalog.models.agg_polars.transform:transform",
        inputs={
            "txs":  FEES_SRC_BINDING.product,
            "gas":  GAS_SRC_BINDING.product,
            "fees": GAS_FEES_SRC_BINDING.product,
        },
        output=AGG,
        out_port="agg",
        dt_param_name="dt",
        input_casts=None,
        required_output_fields=tuple(AggRow.__annotations__.keys()),
    )

    node_binding = bind_polars_model(cfg)

    pipe = PipelineBinding(
        env=ENV,
        registry=REG,
        marker_store=MARKERS,
        default_io=IO,
        sources=(FEES_SRC_BINDING, GAS_SRC_BINDING, GAS_FEES_SRC_BINDING),
        nodes=(node_binding,),
        io_for={},
        run_config=PipelineRunConfig(
            skip=SkipPolicy(location_sensitivity="same_location", allow_copy_from_other_storage=True),
            upstream=UpstreamPolicy(on_missing="run_upstream"),
        ),
    )

    part = Partition({"dt": date.today().isoformat()})
    pipe.run(part)

    out = IO.read(AGG, part)
    print("agg rows:", len(out.rows))
    for r in out.rows[:5]:
        print(r)


if __name__ == "__main__":
    main()
