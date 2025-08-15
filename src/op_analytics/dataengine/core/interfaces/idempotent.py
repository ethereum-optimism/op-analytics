from typing import Protocol
from ..types.dataset import Dataset
from ..types.run_context import RunContext

class Idempotent(Protocol):
    """Capability for steps to expose a stable fingerprint for idempotence."""
    def fingerprint(self, data: Dataset[object], ctx: RunContext) -> str:
        ...
