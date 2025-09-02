import os
from enum import Enum, auto

class OPLabsEnvironment(Enum):
    UNDEFINED = auto()
    UNITTEST = auto()
    DEV = auto()
    PROD = auto()
    CI = auto()

def current_environment() -> OPLabsEnvironment:
    if os.environ.get("PYTEST_VERSION"):
        return OPLabsEnvironment.UNITTEST
    val = (os.environ.get("OPLABS_ENV") or "").upper()
    if val == "PROD":
        return OPLabsEnvironment.PROD
    if val == "CI":
        return OPLabsEnvironment.CI
    if val == "DEV":
        return OPLabsEnvironment.DEV
    return OPLabsEnvironment.UNDEFINED
