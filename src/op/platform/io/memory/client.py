from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Iterable

import fsspec
import polars as pl

from ....core.types.dataset import Dataset
from ....core.types.env import EnvProfile
from ....core.types.file_marker import FileMarker
from ....core.types.partition import Partition
from ....core.types.product_ref import ProductRef
from ....core.types.registry import ProductRegistry
from ....core.types.uri import format_uri
from ....platform.markers.memory_store import MemoryMarkerStore


def _to_uniform_records(rows: Iterable[Any]) -> list[dict[str, Any]]:
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
        # Coerce values to JSON/Parquet-friendly types
        for k, v in list(r.items()):
            if isinstance(v, (date, datetime)):
                r[k] = v.isoformat()
            elif isinstance(v, Decimal):
                r[k] = float(v)
            elif isinstance(v, (set, tuple)):
                r[k] = list(v)
        out.append(r)
    return out


class Client:
    def __init__(self, env: EnvProfile, registry: ProductRegistry, markers: MemoryMarkerStore):
        self.env = env
        self.registry = registry
        self.markers = markers

    def write(self, product: ProductRef, ds: Dataset, part: Partition) -> str:
        loc = self.registry.resolve_write_location(self.env, product)
        base = format_uri(env=self.env, product=product, partition=part, location=loc, wildcard=False)
        if not base.endswith("/"):
            base += "/"
        path = base + "out.parquet"

        # >>> Canonicalize rows to consistent, DataFrame-friendly records
        records = _to_uniform_records(ds.rows)

        # Write parquet to memory:// (or any fsspec filesystem)
        fs, _, paths = fsspec.get_fs_token_paths(path)
        with fs.open(paths[0], "wb") as f:
            # Be explicit about inference across all rows
            pl.DataFrame(records, infer_schema_length=len(records)).write_parquet(f)

        # Emit a FileMarker (storage_id is on the FileLocation)
        fm = FileMarker(
            env=self.env.name,
            product_id=product.id,
            root_path=f"{'/'.join(product.domain)}/{product.name}_{product.version}",
            data_path=path,
            partition=part.values,
            row_count=len(records),
            storage_id=getattr(loc, "storage_id", "mem:test:bronze"),
            location_kind="mem",
            updated_at=datetime.utcnow(),
        )
        self.markers.write_file_markers([fm])
        return path

    def read(self, product: ProductRef, part: Partition) -> Dataset:
        # optional: read back via fsspec memory:// similar to write
        return Dataset(rows=[])
