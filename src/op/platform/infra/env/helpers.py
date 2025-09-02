from .environment import current_environment, OPLabsEnvironment
from .runtime import current_runtime, OPLabsRuntime


def is_k8s() -> bool:
    return current_runtime() == OPLabsRuntime.K8S

def is_github_actions() -> bool:
    return current_runtime() == OPLabsRuntime.GITHUB_ACTIONS

def is_bot() -> bool:
    return is_k8s() or is_github_actions()

def etl_monitor_markers_database() -> str:
    env = current_environment()
    return "etl_monitor_dev" if env == OPLabsEnvironment.UNITTEST else "etl_monitor"
