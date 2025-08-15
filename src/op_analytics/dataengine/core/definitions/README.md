# definitions/

Declarative, serializable pipeline spec.
- `StepRef` — lightweight reference used to assemble pipelines from the registry.
- `StepDefinition` — resolved step + config + constructor bindings.
- `PipelineDefinition` — ordered list of step definitions (list today; DAG later).
