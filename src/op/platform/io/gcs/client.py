from pathlib import Path
from typing import Iterable
import socket, hashlib
from datetime import datetime
import polars as pl
import fsspec

from ....core.interfaces.marker_store import MarkerStore
from ....core.types.file_marker import FileMarker
from ....core.types.partition import Partition
from ....core.types.product_ref import ProductRef


class GcsParquetSink:
    def __init__(self, env: str, bucket_prefix: str | None, marker_store: MarkerStore):
        self.env = env
        self.bucket_prefix = bucket_prefix
        self.marker_store = marker_store

    def _object_uri(self, product: ProductRef, part: Partition) -> str:
        root = resolve_root_path(env=self.env, product=product, bucket_prefix=self.bucket_prefix)
        dt = part.values.get("dt","unpartitioned")
        return f"{root}/dt={dt}/out-{int(datetime.utcnow().timestamp())}.parquet"

    def write(self, product: ProductRef, df: pl.DataFrame, part: Partition) -> list[FileMarker]:
        uri = self._object_uri(product, part)
        fs, _, paths = fsspec.get_fs_token_paths(uri)
        p = paths[0]
        with fs.open(p, "wb") as f:
            df.write_parquet(f)

        # quick stats
        row_count = len(df)
        byte_size = fs.size(p)
        fp = hashlib.sha256(f"{product.id}|{p}|{row_count}|{byte_size}".encode("utf-8")).hexdigest()[:16]

        fm = FileMarker(
            env=self.env,
            product_id=product.id,
            root_path=product.root_path,
            data_path=uri,
            partition=part.values,
            row_count=row_count,
            byte_size=byte_size,
            span_kind=None, span_min=None, span_max=None,
            num_parts=1,
            process_name="gcs_parquet_sink",
            writer_name=socket.gethostname(),
            updated_at=datetime.utcnow(),
            run_id="", fingerprint=fp,
            upstream=tuple(sorted(product.upstream_ids)),
        )
        self.marker_store.write_file_markers([fm])
        return [fm]
