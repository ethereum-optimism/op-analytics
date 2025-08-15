# runtime/

Engine and pipeline execution with idempotence as middleware.
- `DataEngine` — holds registries and constructs `Pipeline`s.
- `Pipeline` — runs steps sequentially with a `RunContext`.
- Materialization: if a step is both a `Sink` and `Idempotent`, the pipeline performs marker read/skip/write **and** registers a `ProductRef` in the `DatasetCatalog` when available.
