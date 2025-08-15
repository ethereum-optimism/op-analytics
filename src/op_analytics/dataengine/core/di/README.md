# di/

Type‑safe class registration and pipeline construction.
- `ClassProvider[T]` — holds a class + default kwargs; creates instances on demand.
- `ComponentRegistry` — registers sources/steps, a marker store, and an optional dataset catalog; can construct a `DataEngine` or Pipeline from `StepRef`s.
