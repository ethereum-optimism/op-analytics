# DataEngine – Typed, Idempotent Pipeline Scaffold

This package provides a strongly‑typed scaffold for building modular, idempotent data pipelines.

**Key ideas**
- Each pipeline step is a typed function object (`Step[I,O]`) that transforms `Dataset[I]` → `Dataset[O]`.
- Idempotence is enforced by middleware using a `MarkerStore` (engine‑level concern) and **markers are written only at materialization boundaries (sinks)**.
- **Publication‑to‑Consumption**: Sinks can **publish** a `ProductRef` into a `DatasetCatalog`; downstream pipelines can reference that product via a `CatalogSourceStep` and orchestrators can **wait** on markers for that product.
- A `ComponentRegistry` registers classes and constructs pipelines from lightweight `StepRef`s.

**What’s included**
- Types & interfaces (`types/`, `interfaces/`)
- Declarative definitions (`definitions/`)
- DI registry (`di/`)
- Runtime engine & idempotence middleware (`runtime/`)
- Marker stores (`marker/`)
- **Dataset catalog** interface & in‑memory impl (`catalog/`)
- Example steps & a runnable example (`steps/`, `examples/`)
- Orchestrator adapter stubs (`orchestration/`)

See directory READMEs for design notes.
