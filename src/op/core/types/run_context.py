from dataclasses import dataclass

@dataclass(frozen=True)
class RunContext:
    env: str
    run_id: str
