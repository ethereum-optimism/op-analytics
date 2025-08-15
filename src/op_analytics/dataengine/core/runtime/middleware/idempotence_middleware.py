from op_analytics.dataengine.core.interfaces.idempotent import Idempotent
from op_analytics.dataengine.core.interfaces.marker_store import MarkerStore
from op_analytics.dataengine.core.interfaces.step import Step
from op_analytics.dataengine.core.types.dataset import Dataset
from op_analytics.dataengine.core.types.run_context import RunContext


class IdempotenceMiddleware:
    """Example callable wrapper that enforces idempotence around a Step implementing Idempotent."""
    def __init__(self, store: MarkerStore) -> None:
        self._store = store

    def __call__(self, step: Step[object, object]) -> Step[object, object]:
        if not isinstance(step, Idempotent):
            return step
        def wrapped(data: Dataset[object], ctx: RunContext) -> Dataset[object]:
            fp = step.fingerprint(data, ctx)
            key = {"pipeline": ctx.pipeline, "step": ctx.step_id, **ctx.partition.values,
                   "schema": data.schema, "schema_version": data.schema_version}
            if self._store.get(key) == fp:
                return data
            out = step.run(data, ctx)
            self._store.put(key, fp)
            return out
        # type: ignore[return-value]
        return type("WrappedStep", (), {"run": wrapped})()  # minimal callable wrapper
