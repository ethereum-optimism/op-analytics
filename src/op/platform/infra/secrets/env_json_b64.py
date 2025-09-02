import base64, json, os
from threading import RLock
from typing import Optional

from .store import Store
from op_analytics.coreutils.path import repo_path  # keep if you want .env discovery

class EnvJsonB64(Store):
    """Reads base64-encoded JSON from OP_ANALYTICS_VAULT (env or .env)."""

    VAR = "OP_ANALYTICS_VAULT"

    def __init__(self, read_dotenv: bool = True) -> None:
        self._lock = RLock()
        self._cache: Optional[dict[str, str]] = None
        self._read_dotenv = read_dotenv

    def _load(self) -> dict[str, str]:
        # Order: .env (optional) → os.environ → default "{}"
        raw: Optional[str] = None
        if self._read_dotenv:
            path = repo_path(".env")
            if path and os.path.isfile(path):
                with open(path, "r", encoding="utf-8") as f:
                    for line in f:
                        if "=" in line:
                            k, v = line.split("=", 1)
                            if k.strip() == self.VAR:
                                raw = v.strip()
                                break
        if raw is None:
            raw = os.environ.get(self.VAR, base64.b64encode(b"{}").decode())

        # Decode (fixes the original bug: decode the *argument*, not a free var)
        decoded = base64.b64decode(raw).decode()
        data = json.loads(decoded)
        if not isinstance(data, dict):
            raise ValueError("OP_ANALYTICS_VAULT must decode to a JSON object")
        # normalize to str→str
        return {str(k): str(v) for k, v in data.items()}

    def _ensure(self) -> dict[str, str]:
        with self._lock:
            if self._cache is None:
                self._cache = self._load()
            return self._cache

    def get(self, key: str) -> str:
        store = self._ensure()
        if key not in store:
            raise KeyError(f"secret not found: {key}")
        return store[key]

    def try_get(self, key: str) -> Optional[str]:
        return self._ensure().get(key)

    def snapshot(self) -> dict[str, str]:
        return dict(self._ensure())
