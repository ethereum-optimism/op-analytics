import os
from enum import Enum


class OPLabsEnvironment(Enum):
    UNDEFINED = 0
    UNITTEST = 1
    DEV = 2
    PROD = 3


_CURRENT_ENV = None


def current_environment():
    global _CURRENT_ENV

    if _CURRENT_ENV is None:
        if os.environ.get("PYTEST_VERSION") is not None:
            _CURRENT_ENV = OPLabsEnvironment.UNITTEST

        elif os.environ.get("OPLABS_ENV").upper() == "PROD":
            _CURRENT_ENV = OPLabsEnvironment.PROD

        else:
            _CURRENT_ENV = OPLabsEnvironment.UNDEFINED

    return _CURRENT_ENV
