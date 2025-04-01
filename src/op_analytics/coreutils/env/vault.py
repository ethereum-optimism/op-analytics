import json
import base64
import os

from op_analytics.coreutils.env.aware import is_k8s
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.path import repo_path

log = structlog.get_logger()

_STORE: dict | None = None


VAULT_ENV_VAR = "OP_ANALYTICS_VAULT"


def load_vault_env_var() -> str | None:
    """Load environment variables.

    At the moment this is only used to pick up the value of the OP_ANALYTICS_VAULT
    environment variable.
    """
    dotenv_path = repo_path(".env")

    vault_env_var: str | None = None

    if dotenv_path is not None and os.path.isfile(dotenv_path):
        with open(dotenv_path, "r") as fobj:
            for line in fobj:
                key, val = line.split("=", maxsplit=1)
                if key == VAULT_ENV_VAR:
                    log.info("loaded vault from .env file")
                    vault_env_var = val.strip()

    if vault_env_var is None and VAULT_ENV_VAR in os.environ:
        log.info("loaded vault from environment")
        vault_env_var = os.environ[VAULT_ENV_VAR]

    if vault_env_var is None:
        log.info("could not load vault env var")

    return vault_env_var


def load_vault() -> dict:
    default: str = base64.b64encode("{}".encode()).decode()

    def _decode(x):
        return json.loads(base64.b64decode(raw).decode())

    if is_k8s():
        with open("/var/secrets/op-analytics-vault.txt", "r") as fobj:
            log.info("loaded vault from secrets volume")
            raw = fobj.read()
            result = _decode(raw)
    else:
        raw = load_vault_env_var() or default
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
    log.debug(f"loaded vault: {len(_STORE)} items")


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


def sanitize_string(s: str) -> str:
    """Sanitize a string for logging.

    This will replace all vault values with a <key> string.
    """
    init()

    for key, val in _STORE.values():
        if isinstance(val, str):
            s.replace(val, f"<{key}>")

    return s
