import json
import base64

from op_analytics.coreutils.env.aware import is_k8s
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.path import repo_path

log = structlog.get_logger()

_STORE: dict | None = None


def load_dotenv() -> dict:
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

    if is_k8s():
        with open("/var/secrets/op-analytics-vault.txt", "r") as fobj:
            raw = fobj.read()
            result = json.loads(base64.b64decode(raw).decode())
    else:
        dotenv = load_dotenv()
        var_name = "OP_ANALYTICS_VAULT"
        result = json.loads(base64.b64decode(dotenv.get(var_name, default)).decode())

    if not isinstance(result, dict):
        # FOR SECURITY DO NOT PRINT THE LOADED "result".
        raise ValueError(f"was expecting a dictionary at {var_name}")
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
