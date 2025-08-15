from dataclasses import dataclass
from typing import List
from .step_definition import StepDefinition

@dataclass(frozen=True)
class PipelineDefinition:
    """Ordered list of step definitions composing a linear pipeline."""
    name: str
    steps: List[StepDefinition]
