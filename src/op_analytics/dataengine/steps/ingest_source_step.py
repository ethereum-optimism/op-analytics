from typing import Generic, List, TypeVar

from op_analytics.dataengine.core.interfaces.source import ISource
from op_analytics.dataengine.core.interfaces.step import Step
from op_analytics.dataengine.core.types.dataset import Dataset
from op_analytics.dataengine.core.types.run_context import RunContext

T = TypeVar("T")

class IngestSourceStep(Step[object, T], Generic[T]):
    """Source step: ignores input and emits Dataset[T] by pulling from an ISource[T]."""
    def __init__(self, source: ISource[T], version: str = "v1") -> None:
        self._source = source
        self._version = version

    def run(self, data: Dataset[object], ctx: RunContext) -> Dataset[T]:
        rows: List[T] = list(self._source.fetch(ctx.partition))
        return Dataset(rows=rows, schema="source", schema_version=self._version)
