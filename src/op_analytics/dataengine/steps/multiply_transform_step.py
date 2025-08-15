from op_analytics.dataengine.core.interfaces.step import Step
from op_analytics.dataengine.core.types.dataset import Dataset
from op_analytics.dataengine.core.types.run_context import RunContext


class MultiplyTransformStep(Step[int, int]):
    """Pure transform: multiplies integer rows by a factor; returns new Dataset[int]."""
    def __init__(self, factor: int = 10) -> None:
        self._factor = factor

    def run(self, data: Dataset[int], ctx: RunContext) -> Dataset[int]:
        return Dataset(
            rows=[r * self._factor for r in data.rows],
            schema=data.schema,
            schema_version=data.schema_version
        )
