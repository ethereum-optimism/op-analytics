from dataclasses import dataclass
from typing import Generic, Optional, Tuple, Type, TypeVar

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class ProductRef(Generic[T]):
    """
    Minimal, stable logical identifier for a data product.

    - domain: hierarchical namespace (e.g., ("defillama", "fees"))
    - name: product short name (e.g., "raw_tvl", "categories", "enriched")
    - version: semver-like or "vN" (e.g., "v1")
    - row_type: optional Python type for the product's row schema (dataclass / TypedDict)

    NOTE: No physical/storage fields here. The catalog/IO managers use ProductRef
    to resolve 'where' (GCS/ClickHouse/Iceberg) at runtime.
    """
    domain: Tuple[str, ...]
    name: str
    version: str
    row_type: Optional[Type[T]] = None

    # ---- Canonical identity / paths ----
    @property
    def path(self) -> Tuple[str, ...]:
        # Used for Dagster AssetKey and marker/caching keys
        return (*self.domain, self.name, self.version)

    @property
    def id(self) -> str:
        # Stable string id for logs, catalogs, and marker store
        return "/".join(self.path)

    # ---- Convenience helpers ----
    def with_version(self, version: str) -> "ProductRef[T]":
        return ProductRef[T](domain=self.domain, name=self.name, version=version, row_type=self.row_type)

    def with_row_type(self, typ: Type[T]) -> "ProductRef[T]":
        return ProductRef[T](domain=self.domain, name=self.name, version=self.version, row_type=typ)

    # Optional: Dagster interop (kept lazy to avoid hard dependency at import)
    def to_asset_key(self):
        try:
            from dagster import AssetKey  # type: ignore
        except Exception as e:
            raise RuntimeError("Dagster not installed; cannot create AssetKey") from e
        return AssetKey(list(self.path))

    @staticmethod
    def from_asset_key(asset_key) -> "ProductRef[object]":
        # Expect AssetKey.path like [*domain, name, version]
        parts = tuple(asset_key.path)
        assert len(parts) >= 3, f"Invalid AssetKey: {parts}"
        domain, name, version = parts[:-2], parts[-2], parts[-1]
        return ProductRef(domain=domain, name=name, version=version)
