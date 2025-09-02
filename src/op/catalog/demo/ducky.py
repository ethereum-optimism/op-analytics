from dataclasses import dataclass
from datetime import date

from op.core.defs.node_binding import NodeBinding
from op.core.defs.product_contract import ProductContract
from op.core.interfaces.node import Node
from op.core.pipeline.pipeline_binding import PipelineBinding
from op.core.types.dataset import Dataset
from op.core.types.env import EnvProfile
from op.core.types.location import FileLocation
from op.core.types.partition import Partition
from op.core.types.policies import PipelineRunConfig, SkipPolicy, UpstreamPolicy
from op.core.types.product_ref import ProductRef
from op.core.types.registry import ProductRegistry, ProductRegistration
from op.platform.binders.sql import bind_sql_source, contract_from_source
from op.platform.configs.bindings import SqlSourceConfig
from op.platform.io.memory.client import Client
from op.platform.markers.memory_store import MemoryMarkerStore


@dataclass(frozen=True)
class FeeRow:
    protocol_id: str
    dt: str
    fee_usd: float

class MapRawToFee(Node[dict[ProductRef, Dataset], dict[ProductRef, Dataset]]):
    def __init__(self, src: ProductRef, dst: ProductRef) -> None:
        self.src, self.dst = src, dst
    def execute(self, input, part):
        out = []
        for r in input[self.src].rows:
            out.append(FeeRow(
                protocol_id=r.get("blockchain",""),
                dt=str(r["dt"]),
                fee_usd=float(r["tx_fee_usd"]),
            ))
        return {self.dst: Dataset(rows=[vars(x) for x in out])}

def main():
    ENV = EnvProfile("test", vars={"root_prefix": "mem"})
    REG = ProductRegistry(_entries={})
    MARKERS = MemoryMarkerStore()

    RAW = ProductRef(domain=("raw","dune"), name="gas_fees_daily", version="v1")
    CUR = ProductRef(domain=("curated","dune"), name="fees_daily", version="v1")

    REG.register(ProductRegistration(
        product=RAW,
        by_env={"test": FileLocation(
            storage_id="mem:test:bronze",
            uri_template="memory:///{root_prefix}/{domain}/{name}/{version}/dt={dt}/",
            format="parquet",
        )}
    ))
    REG.register(ProductRegistration(
        product=CUR,
        by_env={"test": FileLocation(
            storage_id="mem:test:silver",
            uri_template="memory:///{root_prefix}/{domain}/{name}/{version}/dt={dt}/",
            format="parquet",
        )}
    ))

    io = Client(ENV, REG, MARKERS)

    # Source binding (mock mode)
    SRC = bind_sql_source(SqlSourceConfig(
        product=RAW,
        recipe_pkg="op.catalog.raw.dune.fees",   # has config/schema/mock_data/templates
        runner=None, use_mock_data=True,
        defaults={"trailing_days": 2, "ending_days": 0, "single_chain": "none"},
    ))

    # Optional: validate source rows too
    RAW_CONTRACT = contract_from_source(SRC, required=("blockchain","dt","tx_fee_usd"))
    CUR_CONTRACT = ProductContract(product=CUR, row_type=FeeRow, required_fields=("protocol_id","dt","fee_usd"))

    node = MapRawToFee(src=RAW, dst=CUR)
    NB   = NodeBinding(node=node, requires=(RAW_CONTRACT,), provides=(CUR_CONTRACT,))

    run_cfg = PipelineRunConfig(
        skip=SkipPolicy(location_sensitivity="same_location", allow_copy_from_other_storage=True),
        upstream=UpstreamPolicy(on_missing="run_upstream"),
    )

    pipe = PipelineBinding(
        env=ENV,
        registry=REG,
        marker_store=MARKERS,
        default_io=io,
        sources=(SRC,),
        nodes=(NB,),
        io_for={},                  # use default IO
        run_config=run_cfg,
        source_contracts={RAW: RAW_CONTRACT},   # enable source validation
    )

    part = Partition({"dt": date.today().isoformat()})
    pipe.run(part)

    print("RAW markers:", MARKERS.latest_files(RAW.id, where=part.values))
    print("CUR markers:", MARKERS.latest_files(CUR.id,  where=part.values))

if __name__ == "__main__":
    main()