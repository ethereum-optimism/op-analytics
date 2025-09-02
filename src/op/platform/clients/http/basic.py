import json as jsonlib
from typing import Any, Mapping, Optional
from urllib.parse import urlencode
from urllib.request import Request, urlopen  # nosec
from urllib.error import HTTPError, URLError

from ....core.interfaces.http_runner import HttpRunner

class BasicHttpRunner(HttpRunner):
    def __init__(self, timeout_sec: float = 30.0, user_agent: str = "op-analytics/1.0"):
        self._timeout = timeout_sec
        self._ua = user_agent

    def request_json(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
        json: Any | None = None,              # keep name for compatibility
        timeout: float | None = None,
    ) -> Any:
        qs = f"?{urlencode(params)}" if params else ""
        full = f"{url}{qs}"
        body_bytes = None
        hdrs = {"User-Agent": self._ua, "Accept": "application/json"}
        if headers:
            hdrs.update(headers)
        if json is not None:
            body_bytes = jsonlib.dumps(json).encode("utf-8")
            hdrs["Content-Type"] = "application/json"

        req = Request(full, data=body_bytes, headers=hdrs, method=method.upper())
        try:
            with urlopen(req, timeout=timeout or self._timeout) as resp:  # nosec
                ct = (resp.headers.get("Content-Type") or "").lower()
                raw = resp.read()
                # Handle JSON even if Content-Type doesn't include charset strictly
                if "application/json" in ct or raw.strip().startswith((b"{", b"[")):
                    return jsonlib.loads(raw.decode("utf-8"))
                return {"raw": raw.decode("utf-8")}
        except HTTPError as e:
            body = e.read().decode("utf-8", errors="ignore")
            raise RuntimeError(f"HTTP {e.code} for {full}: {body}") from e
        except URLError as e:
            raise RuntimeError(f"HTTP error for {full}: {e}") from e
