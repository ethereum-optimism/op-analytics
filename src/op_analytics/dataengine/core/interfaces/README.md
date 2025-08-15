# interfaces/

Protocol/ABC contracts for step logic, idempotence, persistence, sources, and publication.
- `ISource[T]` — abstract source of typed rows per partition.
- `Step[I,O]` — protocol for a typed step transformation.
- `Idempotent` — capability for sinks to provide a stable fingerprint used by middleware.
- `Sink[T]` — protocol for side‑effecting sinks (materialization boundary).
- `MarkerStore` — engine‑level store for idempotence markers.
- `Publishable` — capability for sinks to emit a `ProductRef` describing their output dataset.
- `DatasetCatalog` — registry that records and resolves `ProductRef`s so other pipelines can consume them.
