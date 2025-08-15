# types/

Small, strongly‑typed value objects used across the engine.
- `Partition` — structured partition context (e.g., `{ "dt": "2025-08-12" }`).
- `RunContext` — per‑step execution context (pipeline, step, partition).
- `Dataset[T]` — typed rows with schema metadata (flows between steps).
- `ProductRef` — portable handle to a published dataset (name, version, storage kind, locator).
