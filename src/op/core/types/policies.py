from dataclasses import dataclass
from typing import Literal

@dataclass(frozen=True)
class SkipPolicy:
    """
    Controls when we skip/copy/recompute if materialization exists.
      - location_sensitivity:
          "same_location"  -> skip only if written to the *same* storage_id
          "any_location"   -> skip if written *anywhere*
          "none"           -> never skip (always recompute)
      - allow_copy_from_other_storage:
          if True and we have an artifact in a different storage, copy it instead of recomputing
      - require_fingerprint_match:
          if True, only skip/copy when marker.fingerprint == expected_fingerprint
          (leave False until you wire full fingerprinting end-to-end)
    """
    location_sensitivity: Literal["same_location","any_location","none"] = "same_location"
    allow_copy_from_other_storage: bool = True
    require_fingerprint_match: bool = False  # default lenient until fingerprints are everywhere

@dataclass(frozen=True)
class UpstreamPolicy:
    """
    Controls what happens if required inputs are not present at runtime.
      - on_missing:
          "run_upstream" -> include upstream producers (transitively)
          "error"        -> fail fast when inputs are missing
          "skip_target"  -> drop the target step from the run
    """
    on_missing: Literal["run_upstream","error","skip_target"] = "run_upstream"
    max_depth: int = 100

@dataclass(frozen=True)
class PipelineRunConfig:
    skip: SkipPolicy = SkipPolicy()
    upstream: UpstreamPolicy = UpstreamPolicy()
