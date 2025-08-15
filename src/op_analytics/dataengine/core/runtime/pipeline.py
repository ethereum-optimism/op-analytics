from op_analytics.dataengine.core.definitions.pipeline_definition import PipelineDefinition
from op_analytics.dataengine.core.definitions.step_definition import StepDefinition
from op_analytics.dataengine.core.di.class_provider import ClassProvider
from op_analytics.dataengine.core.interfaces.idempotent import Idempotent
from op_analytics.dataengine.core.interfaces.publishable import Publishable
from op_analytics.dataengine.core.interfaces.sink import Sink
from op_analytics.dataengine.core.interfaces.step import Step
from op_analytics.dataengine.core.types.dataset import Dataset
from op_analytics.dataengine.core.types.partition import Partition
from op_analytics.dataengine.core.types.run_context import RunContext


class Pipeline:
    """Executes steps in order; marks and catalog‑publishes only at sinks."""
    def __init__(self, defn: PipelineDefinition, engine) -> None:
        self._defn = defn
        self._engine = engine

    def _instantiate(self, sd: StepDefinition) -> Step[object, object]:
        provider: ClassProvider[Step[object, object]] = self._engine.steps[sd.implementation_name]
        ctor_kwargs = dict(provider.kwargs)
        if sd.bindings:
            for param, dep_name in sd.bindings.items():
                ctor_kwargs[param] = self._engine.sources[dep_name].create()
        return provider.cls(**ctor_kwargs)

    def run(self, partition: Partition) -> Dataset[object]:
        data: Dataset[object] = Dataset(rows=[], schema="", schema_version="")
        for sd in self._defn.steps:
            step = self._instantiate(sd)
            ctx = RunContext(pipeline=self._defn.name, step_id=sd.step_id, partition=partition)

            # Materialization boundary: Sink + Idempotent → markers (and optional catalog publish)
            if isinstance(step, Sink) and isinstance(step, Idempotent):
                fp = step.fingerprint(data, ctx)
                key = {
                    "pipeline": self._defn.name,
                    "step": sd.step_id,
                    **partition.values,
                    "schema": data.schema,
                    "schema_version": data.schema_version,
                }
                if self._engine.marker_store.get(key) == fp:
                    # already materialized; no re‑write
                    continue
                out = step.run(data, ctx)
                self._engine.marker_store.put(key, fp)

                # If the sink is publishable and a catalog is configured, register the product
                if self._engine.catalog and isinstance(step, Publishable):
                    product = step.product_ref()
                    self._engine.catalog.register(product, partition.values, fp)
                data = out
            else:
                data = step.run(data, ctx)
        return data