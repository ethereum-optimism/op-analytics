from dataclasses import dataclass
from typing import Mapping

@dataclass(frozen=True)
class ProductRef:
    """Portable handle to a published dataset (name, version, storage kind, and locator)."""
    name: str              # logical dataset name, e.g. "blocks_with_fees"
    version: str           # semantic dataset version
    storage_kind: str      # e.g. "clickhouse", "gcs", "s3"
    locator: Mapping[str, str]  # provider-specific locator (e.g., {"table": "db.tbl"})
