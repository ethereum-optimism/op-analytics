from typing import Any, Mapping, Protocol

class HttpRunner(Protocol):
    """Minimal HTTP client that returns parsed JSON."""
    def request_json(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
        json: Any | None = None,
        timeout: float | None = None,
    ) -> Any: ...
