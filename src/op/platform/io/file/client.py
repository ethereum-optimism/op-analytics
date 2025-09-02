import polars as pl
from typing import List

from ....core.types.catalog import Catalog
from ....core.types.dataset import Dataset
from ....core.types.env import EnvProfile
from ....core.types.location import FileLocation
from ....core.types.partition import Partition
from ....core.types.product_ref import ProductRef
from ....core.types.uri import format_uri


class FileProductIO:
    def __init__(self, env: EnvProfile, catalog: Catalog):
        self.env, self.catalog = env, catalog

    def read(self, product: ProductRef, part: Partition):
        frames: List[pl.DataFrame] = []
        for loc in self.catalog.resolve_read_locations(self.env, product):
            if isinstance(loc, FileLocation):
                glob = format_uri(self.env, product, part, loc, wildcard=True)
                try:
                    frames.append(pl.scan_parquet(glob).collect())
                except Exception:
                    # tolerate missing legacy paths
                    continue
        if not frames:
            return Dataset(rows=[])
        df = pl.concat(frames, how="vertical_relaxed")
        return Dataset(rows=df.to_dicts())
