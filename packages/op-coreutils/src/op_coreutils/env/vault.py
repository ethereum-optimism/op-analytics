import json
import os
import base64

from op_coreutils.logger import structlog

log = structlog.get_logger()

_STORE: dict | None = None


def init() -> None:
    """Load the secrets into the vault store."""
    global _STORE

    if _STORE is not None:
        # Only initialize  once.
        return

    default: bytes = base64.b64encode("{}".encode())
    data = json.loads(base64.b64decode(os.environ.get("OP_ANALYTICS_VAULT", default)).decode())

    _STORE = {}
    for key, val in data.items():
        _STORE[key] = val
    log.info(f"Loaded {len(_STORE)} items into vault.")


def env_get(key: str):
    init()

    if _STORE is None:
        raise ValueError("OP_ANALYTICS_VAULT was not propertly initialized.")

    return _STORE[key]


def env_get_or_none(key: str):
    init()

    if _STORE is None:
        raise ValueError("OP_ANALYTICS_VAULT was not propertly initialized.")

    return _STORE.get(key)
