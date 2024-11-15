import json
import base64

from op_analytics.coreutils.env.aware import is_k8s
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.path import repo_path

log = structlog.get_logger()

_STORE: dict | None = None


def load_dotenv() -> dict:
    """Load env vars from the .env file.

    At the moment this is only used to pick up the value of the OP_ANALYTICS_VAULT
    environment variable.
    """
    dotenv_path = repo_path(".env")

    if dotenv_path is None:
        log.error("Did not find .env. No env vars will be loaded.")
        return {}

    result = {}
    with open(dotenv_path, "r") as fobj:
        for line in fobj:
            key, val = line.split("=", maxsplit=1)
            result[key] = val.strip()

    return result


def load_vault() -> dict:
    default: bytes = base64.b64encode("{}".encode())

    def _decode(x):
        return json.loads(base64.b64decode(raw).decode())

    if is_k8s():
        with open("/var/secrets/op-analytics-vault.txt", "r") as fobj:
            raw = fobj.read()
            result = _decode(raw)
    else:
        raw = load_dotenv().get("OP_ANALYTICS_VAULT", default)
        result = _decode(raw)

    if not isinstance(result, dict):
        # FOR SECURITY DO NOT PRINT THE LOADED "result".
        raise ValueError("was expecting a dictionary")
    return result


def init() -> None:
    """Load the secrets into the vault store."""
    global _STORE

    if _STORE is not None:
        # Only initialize  once.
        return

    data = load_vault()

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
