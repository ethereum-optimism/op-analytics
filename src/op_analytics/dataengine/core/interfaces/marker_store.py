from typing import Mapping, Optional, Protocol

class MarkerStore(Protocol):
    """Store for idempotence markers keyed by a stable set of attributes."""
    def get(self, key: Mapping[str, str]) -> Optional[str]:
        ...
    def put(self, key: Mapping[str, str], fingerprint: str) -> str:
        ...
