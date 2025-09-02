from typing import Optional

from .caching import Caching
from .composite import Composite
from .env_json_b64 import EnvJsonB64
from .k8s_file import K8sFile
from .store import Store
from ..env.runtime import current_runtime, OPLabsRuntime

_DEFAULT: Optional[Store] = None

def build_default_store() -> Store:
    """K8s → Env; wrap with an in-process cache."""
    runtime = current_runtime()
    chain: Store
    if runtime == OPLabsRuntime.K8S:
        chain = Composite(K8sFile(), EnvJsonB64())
    else:
        chain = EnvJsonB64()
    return Caching(chain)

def default_store() -> Store:
    global _DEFAULT
    if _DEFAULT is None:
        _DEFAULT = build_default_store()
    return _DEFAULT

def env_get(key: str) -> str:
    return default_store().get(key)

def env_get_or_none(key: str) -> Optional[str]:
    return default_store().try_get(key)