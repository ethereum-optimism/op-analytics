from typing import Optional

from .store import Store


class Composite(Store):
    """Tries a list of stores in order (first hit wins)."""
    def __init__(self, *stores: Store) -> None:
        self._stores = list(stores)

    def get(self, key: str) -> str:
        for s in self._stores:
            val = s.try_get(key)
            if val is not None:
                return val
        raise KeyError(f"secret not found in any store: {key}")

    def try_get(self, key: str) -> Optional[str]:
        for s in self._stores:
            val = s.try_get(key)
            if val is not None:
                return val
        return None

    def snapshot(self) -> dict[str, str]:
        snap: dict[str, str] = {}
        for s in self._stores:
            snap.update(s.snapshot())
        return snap