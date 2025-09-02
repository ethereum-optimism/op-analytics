from __future__ import annotations

from dataclasses import asdict, is_dataclass, replace
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Iterable

import fsspec
import polars as pl

from ....core.interfaces.product_io import ProductIO
from ....core.types.dataset import Dataset
from ....core.types.env import EnvProfile
from ....core.types.file_marker import FileMarker
from ....core.types.partition import Partition
from ....core.types.product_ref import ProductRef
from ....core.types.registry import ProductRegistry
from ....core.types.uri import format_uri
from ....platform.markers.memory_store import MemoryMarkerStore  # or your real MarkerStore


def _to_uniform_records(rows: Iterable[Any]) -> list[dict[str, Any]]:
    """Normalize rows so Polars can infer a consistent schema (dates -> isoformat, Decimals -> float)."""
    out: list[dict[str, Any]] = []
    for r in rows:
        if is_dataclass(r):
            r = asdict(r)
        elif hasattr(r, "__dict__") and not isinstance(r, dict):
            r = vars(r)
        elif isinstance(r, dict):
            r = dict(r)
        else:
            raise TypeError(f"Unsupported row type: {type(r)}")

        for k, v in list(r.items()):
            if isinstance(v, (date, datetime)):
                r[k] = v.isoformat()
            elif isinstance(v, Decimal):
                r[k] = float(v)
            elif isinstance(v, (set, tuple)):
                r[k] = list(v)
        out.append(r)
    return out


class Client(ProductIO):
    """
    Local FS IO client: writes Parquet to `file:///...` (or plain paths),
    and emits FileMarkers to a provided MarkerStore.
    """

    def __init__(self, env: EnvProfile, registry: ProductRegistry, markers: MemoryMarkerStore):
        self.env = env
        self.registry = registry
        self.markers = markers

    def write(self, product: ProductRef, ds: Dataset, part: Partition) -> str:
        loc = self.registry.resolve_write_location(self.env, product)
        # Resolve directory path, ensure trailing slash
        base = format_uri(env=self.env, product=product, partition=part, location=loc, wildcard=False)
        if not base.endswith("/"):
            base += "/"
        # Stable filename; you can add timestamp or partition suffix if you want multiple parts
        path = base + "part-000.parquet"

        # Normalize rows -> DataFrame
        records = _to_uniform_records(ds.rows)
        df = pl.DataFrame(records, infer_schema_length=len(records)) if records else pl.DataFrame([])

        # fsspec works for both file:/// and plain /tmp paths
        fs, _, paths = fsspec.get_fs_token_paths(path)
        with fs.open(paths[0], "wb") as f:
            df.write_parquet(f)

        # Emit marker with storage_id from the FileLocation
        storage_id = getattr(loc, "storage_id", "fs:local:unknown")
        fm = FileMarker(
            env=self.env.name,
            product_id=product.id,
            root_path=f"{'/'.join(product.domain)}/{product.name}_{product.version}",
            data_path=path,
            partition=part.values,
            row_count=len(records),
            byte_size=None,
            span_min=None,
            span_max=None,
            span_kind=None,
            num_parts=1,
            process_name="local.fs.client",
            writer_name="local",
            updated_at=datetime.utcnow(),
            run_id="",
            fingerprint="",    # set if you compute an algo/upstream fp
            upstream=tuple(),
            storage_id=storage_id,
            location_kind="fs",
        )
        self.markers.write_file_markers([fm])
        return path

    def read(self, product: ProductRef, part: Partition) -> Dataset:
        """Optional: implement if you want to load what you wrote (can scan with wildcard)."""
        return Dataset(rows=[])
