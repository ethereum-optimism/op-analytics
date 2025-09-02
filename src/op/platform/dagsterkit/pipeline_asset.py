from typing import Iterable, Optional
import dagster as dg

from op.core.pipeline.pipeline_binding import PipelineBinding
from op.core.types.product_ref import ProductRef
from op.core.types.partition import Partition
from op.core.interfaces.product_io import ProductIO
from op.platform.io.ephemeral.client import EphemeralIO

def _product_asset_key(p: ProductRef) -> dg.AssetKey:
    return dg.AssetKey(list(p.domain) + [p.name, p.version])

def pipeline_asset_for_product(
    *,
    binding: PipelineBinding,
    target: ProductRef,
    upstream_products: Iterable[ProductRef],
    partitions_def: Optional[dg.PartitionsDefinition] = None,
    freshness_minutes: Optional[int] = None,
    product_io_resource_key: str = "product_io",
    description: Optional[str] = None,
    ephemeral_products: Iterable[ProductRef] = (),   # <-- NEW
) -> dg.AssetsDefinition:
    asset_key = _product_asset_key(target)
    deps = [_product_asset_key(p) for p in upstream_products]

    @dg.asset(
        key=asset_key,
        deps=deps,
        partitions_def=partitions_def,
        required_resource_keys={product_io_resource_key},
        compute_kind="python",
        freshness_policy=(
            dg.FreshnessPolicy(maximum_lag_minutes=freshness_minutes)
            if freshness_minutes else None
        ),
        description=description or f"Materializes {target.id} via pipeline.run_one()",
    )
    def _asset(context: dg.AssetExecutionContext):
        pio_remote: ProductIO = getattr(context.resources, product_io_resource_key)
        ephemeral_set = set(ephemeral_products)

        # collect all products the pipeline touches
        products: set[ProductRef] = set()
        for s in binding.sources: products.add(s.product)
        for n in binding.nodes:
            for rc in n.requires:  products.add(rc.product)
            for pc in n.provides:  products.add(pc.product)

        # route ephemerals to in-memory, others to remote
        io_for = {p: (EphemeralIO() if p in ephemeral_set else pio_remote) for p in products}

        runtime_pipe = PipelineBinding(
            sources=binding.sources,
            nodes=binding.nodes,
            io_for=io_for,
        )

        part = Partition({"dt": context.partition_key} if context.has_partition_key else {})
        runtime_pipe.run_one(part, target)

        # Optionally surface remote manifest for final product
        meta = {}
        try:
            meta = pio_remote.manifest(target, part)  # type: ignore[attr-defined]
        except Exception:
            pass
        context.add_output_metadata({k: meta.get(k) for k in ("rows","path","partition")})
        return None

    _asset.__name__ = "asset__" + "__".join(asset_key.path)
    return _asset
