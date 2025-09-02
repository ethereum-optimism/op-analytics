from .store import Store


class SecretSanitizer:
    """Replaces secret values in strings with <KEY> placeholders."""
    def __init__(self, store: Store) -> None:
        self._store = store

    def sanitize(self, text: str) -> str:
        out = text
        for key, val in self._store.snapshot().items():
            if not isinstance(val, str):
                continue
            # heuristics: skip non-secrets if desired
            if val == "default" or key.endswith("_USER") or key.endswith("_PORT"):
                continue
            out = out.replace(val, f"<{key}>")
        return out