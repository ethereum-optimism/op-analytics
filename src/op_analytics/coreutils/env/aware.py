import os
from enum import Enum


class OPLabsEnvironment(Enum):
    UNDEFINED = 0
    UNITTEST = 1
    DEV = 2
    PROD = 3


class OPLabsRuntime(Enum):
    LOCAL = 0
    K8S = 1


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

        else:
            _CURRENT_RUNTIME = OPLabsRuntime.LOCAL

    return _CURRENT_RUNTIME


def is_k8s():
    return True
    # return current_runtime() == OPLabsRuntime.K8S
