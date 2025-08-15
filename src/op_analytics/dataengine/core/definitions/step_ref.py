from dataclasses import dataclass, field
from typing import Dict, Optional

@dataclass(frozen=True)
class StepRef:
    """Lightweight reference to a registered step with config and constructor bindings."""
    name: str
    step_id: Optional[str] = None
    config: Dict[str, object] = field(default_factory=dict)
    bindings: Optional[Dict[str, str]] = None
