from threading import RLock
from typing import Mapping, Optional

from op_analytics.dataengine.core.interfaces.marker_store import MarkerStore


class MemoryMarkerStore(MarkerStore):
    """In‑memory marker store (thread‑safe) for tests and local execution."""
    def __init__(self) -> None:
        self._lock = RLock()
        self._data: dict[tuple[tuple[str, str], ...], str] = {}

    def _k(self, key: Mapping[str, str]) -> tuple[tuple[str, str], ...]:
        return tuple(sorted(key.items()))

    def get(self, key: Mapping[str, str]) -> Optional[str]:
        with self._lock:
            return self._data.get(self._k(key))

    def put(self, key: Mapping[str, str], fingerprint: str) -> str:
        with self._lock:
            self._data[self._k(key)] = fingerprint
            return fingerprint
