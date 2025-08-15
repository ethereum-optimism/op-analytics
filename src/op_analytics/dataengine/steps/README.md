# steps/

Reference step implementations showing the typed flow + publication/consumption:
- `IngestSourceStep[T]` — wraps an `ISource[T]` to emit `Dataset[T]`.
- `MultiplyTransformStep` — pure in‑memory transform (Dataset[int] → Dataset[int]).
- `ClickHouseSinkStep[T]` — persists, publishes a `ProductRef`, and returns the same `Dataset[T]` (sink‑as‑step).
- `CatalogSourceStep[T]` — resolves a `ProductRef` via the catalog and exposes it as a source for downstream pipelines.
