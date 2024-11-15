import os

_REPO_ROOT: str | None = None


def repo_root():
    global _REPO_ROOT

    if _REPO_ROOT is None:
        current: str | None = os.path.dirname(__file__)
        while True:
            if "uv.lock" in os.listdir(current):
                break
            elif "/" == current:
                current = None  # not runnign from the git repo
                break
            else:
                assert current is not None
                current = os.path.dirname(current)

        _REPO_ROOT = current

    return _REPO_ROOT


def repo_path(path: str) -> str | None:
    root = repo_root()
    if root is None:
        return None

    return os.path.join(repo_root(), path)
