from typing import Generic, Protocol, TypeVar

from op_analytics.dataengine.core.types.dataset import Dataset
from op_analytics.dataengine.core.types.run_context import RunContext

I = TypeVar("I")
O = TypeVar("O")

class Step(Protocol, Generic[I, O]):
    """Typed transformation from Dataset[I] → Dataset[O] given a RunContext."""
    def run(self, data: Dataset[I], ctx: RunContext) -> Dataset[O]:
        ...
