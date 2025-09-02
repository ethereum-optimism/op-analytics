from threading import RLock
from typing import Optional

from .store import Store


class Caching(Store):
    """Thread-safe read-through cache wrapper."""
    def __init__(self, inner: Store) -> None:
        self._inner = inner
        self._lock = RLock()
        self._cache: dict[str, str] = {}

    def get(self, key: str) -> str:
        with self._lock:
            if key in self._cache:
                return self._cache[key]
        val = self._inner.get(key)
        with self._lock:
            self._cache[key] = val
        return val

    def try_get(self, key: str) -> Optional[str]:
        with self._lock:
            if key in self._cache:
                return self._cache[key]
        val = self._inner.try_get(key)
        if val is not None:
            with self._lock:
                self._cache[key] = val
        return val

    def snapshot(self) -> dict[str, str]:
        with self._lock:
            # combine cache with inner snapshot for completeness
            snap = dict(self._inner.snapshot())
            snap.update(self._cache)
            return snap
