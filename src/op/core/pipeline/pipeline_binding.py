import os
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Set, Tuple, Literal

import fsspec

from ..defs.node_binding import NodeBinding
from ..defs.product_contract import ProductContract
from ..defs.source_binding import SourceBinding
from ..interfaces.marker_store import MarkerStore
from ..interfaces.product_io import ProductIO

from ..types.dataset import Dataset
from ..types.env import EnvProfile
from ..types.file_marker import FileMarker
from ..types.partition import Partition
from ..types.policies import SkipPolicy, PipelineRunConfig
from ..types.product_ref import ProductRef
from ..types.registry import ProductRegistry
from ..types.uri import format_uri, compute_storage_id


def _latest(markers: MarkerStore, product: ProductRef, part: Partition) -> Optional[FileMarker]:
    found = markers.latest_files(product.id, where=part.values)  # type: ignore[arg-type]
    return found[0] if found else None

def _decide(skip: SkipPolicy, *, current_storage_id: str, markers: MarkerStore, product: ProductRef, part: Partition) -> tuple[Literal["skip","copy","compute"], Optional[FileMarker]]:
    cands = markers.latest_files(product.id, where=part.values)  # type: ignore[arg-type]
    if not cands:
        return "compute", None
    best_same = next((m for m in cands if m.storage_id == current_storage_id), None)
    best_any = cands[0]
    if skip.location_sensitivity == "none":
        return "compute", None
    if skip.location_sensitivity == "same_location":
        if best_same:
            return "skip", None
        if skip.allow_copy_from_other_storage and best_any:
            return "copy", best_any
        return "compute", None
    # any_location
    if best_any:
        if best_any.storage_id != current_storage_id and skip.allow_copy_from_other_storage:
            return "copy", best_any
        return "skip", None
    return "compute", None

def _copy_blob(src_uri: str, dst_uri: str) -> None:
    src_fs, _, [src] = fsspec.get_fs_token_paths(src_uri)
    dst_fs, _, [dst] = fsspec.get_fs_token_paths(dst_uri)
    try:
        dst_fs.mkdirs(os.path.dirname(dst), exist_ok=True)  # type: ignore[attr-defined]
    except Exception:
        pass
    buf = 1024 * 1024
    with src_fs.open(src, "rb") as s, dst_fs.open(dst, "wb") as d:
        while True:
            chunk = s.read(buf)
            if not chunk:
                break
            d.write(chunk)

@dataclass(frozen=True)
class PipelineBinding:
    env: EnvProfile
    registry: ProductRegistry
    marker_store: MarkerStore
    default_io: ProductIO
    sources: Tuple[SourceBinding, ...]
    nodes: Tuple[NodeBinding, ...]
    io_for: Dict[ProductRef, ProductIO]
    run_config: PipelineRunConfig
    # Optional: validate sources too
    source_contracts: Dict[ProductRef, ProductContract] = None  # default None is fine

    # ---- planning ----

    def _producer_by_product(self) -> Dict[ProductRef, str]:
        prod: Dict[ProductRef, str] = {}
        for s in self.sources:
            sid = f"source:{s.name}"
            if s.product in prod:
                raise ValueError(f"Duplicate producer for {s.product.id}")
            prod[s.product] = sid
        for n in self.nodes:
            sid = f"node:{n.node.__class__.__name__}"
            for pc in n.provides:
                if pc.product in prod:
                    raise ValueError(f"Duplicate producer for {pc.product.id}")
                prod[pc.product] = sid
        return prod

    def _edges(self) -> List[Tuple[str, str]]:
        edges: List[Tuple[str, str]] = []
        prod_to_step = self._producer_by_product()
        for n in self.nodes:
            nid = f"node:{n.node.__class__.__name__}"
            for rc in n.requires:
                s = prod_to_step.get(rc.product)
                if s is None:
                    raise ValueError(f"Unproduced requirement {rc.product.id} for node {nid}")
                edges.append((s, nid))
        return edges

    def _all_steps(self) -> Set[str]:
        steps: Set[str] = set()
        for s in self.sources:
            steps.add(f"source:{s.name}")
        for n in self.nodes:
            steps.add(f"node:{n.node.__class__.__name__}")
        return steps

    def _toposort(self) -> List[str]:
        steps = self._all_steps()
        edges = self._edges()
        incoming: Dict[str, int] = {k: 0 for k in steps}
        out: Dict[str, List[str]] = {k: [] for k in steps}
        for a, b in edges:
            out[a].append(b)
            incoming[b] += 1
        ready = [k for k, deg in incoming.items() if deg == 0]
        order: List[str] = []
        while ready:
            a = ready.pop()
            order.append(a)
            for b in out[a]:
                incoming[b] -= 1
                if incoming[b] == 0:
                    ready.append(b)
        if len(order) != len(steps):
            raise ValueError("Cycle detected in pipeline")
        return order

    def _step_lookup(self) -> Dict[str, object]:
        d: Dict[str, object] = {}
        for s in self.sources:
            d[f"source:{s.name}"] = s
        for n in self.nodes:
            d[f"node:{n.node.__class__.__name__}"] = n
        return d

    def _io(self, product: ProductRef) -> ProductIO:
        return self.io_for.get(product, self.default_io)

    # ---- run ----

    def run(self, part: Partition, targets: Iterable[ProductRef] | None = None) -> None:
        order = self._toposort()
        steps = self._step_lookup()
        src_contracts = self.source_contracts or {}

        for sid in order:
            step = steps[sid]

            # Sources
            if isinstance(step, SourceBinding):
                product = step.product
                loc = self.registry.resolve_write_location(self.env, product)
                storage_id = loc.storage_id
                action, copy_from = _decide(self.run_config.skip, current_storage_id=storage_id, markers=self.marker_store, product=product, part=part)
                if action == "skip":
                    continue
                if action == "copy" and copy_from is not None:
                    base = format_uri(env=self.env, product=product, partition=part, location=loc, wildcard=False)
                    if not base.endswith("/"):
                        base += "/"
                    dst = base + os.path.basename(copy_from.data_path.rstrip("/"))
                    _copy_blob(copy_from.data_path, dst)
                    fm = FileMarker(
                        env=self.env.name, product_id=product.id, data_path=dst, partition=part.values,
                        row_count=int(copy_from.row_count or 0), updated_at=datetime.utcnow(),
                        storage_id=storage_id, location_kind="copy", process_name="pipeline.copy", writer_name="pipeline"
                    )
                    self.marker_store.write_file_markers([fm])
                    continue
                rows = list(step.source.resource.fetch(part))
                ds = Dataset(rows=list(rows))
                # optional validation for sources
                sc = src_contracts.get(product)
                if sc:
                    sc.validate_against(ds)
                self._io(product).write(product, ds, part)
                continue

            # Nodes
            if isinstance(step, NodeBinding):
                inputs: Dict[ProductRef, Dataset] = {}
                missing: List[ProductRef] = []
                for rc in step.requires:
                    if _latest(self.marker_store, rc.product, part) is None:
                        missing.append(rc.product)
                    else:
                        inputs[rc.product] = self._io(rc.product).read(rc.product, part)

                mode = self.run_config.upstream.on_missing
                if missing:
                    if mode == "error":
                        raise RuntimeError(f"Missing upstream inputs for {sid}: {[p.id for p in missing]}")
                    if mode == "skip_target":
                        continue
                    raise RuntimeError(f"Upstream 'run_upstream' but inputs missing at execution for {sid}: {[p.id for p in missing]}")

                outputs = step.node.execute(inputs, part)

                # validate + write provided datasets
                for pc in step.provides:
                    product = pc.product
                    ds = outputs.get(product, Dataset(rows=[]))
                    # ensure list for your ProductContract.validate_against
                    if not isinstance(ds.rows, list):
                        ds = Dataset(rows=list(ds.rows))
                    pc.validate_against(ds)
                    self._io(product).write(product, ds, part)
                continue

            raise TypeError(f"Unknown step type: {type(step)}")

    def run_one(self, part: Partition, target: ProductRef) -> None:
        self.run(part, targets=(target,))
