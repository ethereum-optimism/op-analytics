import os
from enum import Enum


class OPLabsEnvironment(Enum):
    UNDEFINED = 0
    UNITTEST = 1
    DEV = 2
    PROD = 3
    CI = 4


class OPLabsRuntime(Enum):
    LOCAL = 0
    K8S = 1
    GITHUB_ACTIONS = 2


_CURRENT_ENV = None
_CURRENT_RUNTIME = None


def current_environment():
    global _CURRENT_ENV

    if _CURRENT_ENV is None:
        oplabs_env = os.environ.get("OPLABS_ENV")

        if os.environ.get("PYTEST_VERSION") is not None:
            _CURRENT_ENV = OPLabsEnvironment.UNITTEST

        elif oplabs_env is not None and oplabs_env.upper() == "PROD":
            _CURRENT_ENV = OPLabsEnvironment.PROD

        else:
            _CURRENT_ENV = OPLabsEnvironment.UNDEFINED

    return _CURRENT_ENV


def current_runtime():
    global _CURRENT_RUNTIME

    if _CURRENT_RUNTIME is None:
        oplabs_runtime = os.environ.get("OPLABS_RUNTIME")

        if oplabs_runtime == "k8s":
            _CURRENT_RUNTIME = OPLabsRuntime.K8S

        elif oplabs_runtime == "gha":
            _CURRENT_RUNTIME = OPLabsRuntime.GITHUB_ACTIONS

        else:
            _CURRENT_RUNTIME = OPLabsRuntime.LOCAL

    return _CURRENT_RUNTIME


def is_k8s():
    return current_runtime() == OPLabsRuntime.K8S


def is_github_actions():
    return current_runtime() == OPLabsRuntime.GITHUB_ACTIONS


def is_bot():
    return is_k8s() or is_github_actions()


def etl_monitor_markers_database():
    current_env = current_environment()
    if current_env == OPLabsEnvironment.UNITTEST:
        return "etl_monitor_dev"
    else:
        return "etl_monitor"
