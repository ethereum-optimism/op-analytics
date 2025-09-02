import base64, json
from threading import RLock
from typing import Optional

from .store import Store


class K8sFile(Store):
    """Reads base64 JSON from a mounted file (e.g., /var/secrets/op-analytics-vault.txt)."""

    def __init__(self, path: str = "/var/secrets/op-analytics-vault.txt") -> None:
        self._path = path
        self._lock = RLock()
        self._cache: Optional[dict[str, str]] = None

    def _load(self) -> dict[str, str]:
        with open(self._path, "r", encoding="utf-8") as f:
            raw = f.read()
        data = json.loads(base64.b64decode(raw).decode())
        if not isinstance(data, dict):
            raise ValueError("vault file must decode to a JSON object")
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
