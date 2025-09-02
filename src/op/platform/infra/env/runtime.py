import os
from enum import Enum, auto

class OPLabsRuntime(Enum):
    LOCAL = auto()
    K8S = auto()
    GITHUB_ACTIONS = auto()

def current_runtime() -> OPLabsRuntime:
    val = os.environ.get("OPLABS_RUNTIME")
    if val == "k8s":
        return OPLabsRuntime.K8S
    if val == "gha":
        return OPLabsRuntime.GITHUB_ACTIONS
    return OPLabsRuntime.LOCAL
