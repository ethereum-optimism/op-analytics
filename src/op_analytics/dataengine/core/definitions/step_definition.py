from dataclasses import dataclass
from typing import Dict, Optional

@dataclass(frozen=True)
class StepDefinition:
    """Resolved step: impl name, config object, and DI constructor bindings."""
    step_id: str
    implementation_name: str
    config: Dict[str, object]
    bindings: Optional[Dict[str, str]] = None
