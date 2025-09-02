import importlib
from dataclasses import asdict, is_dataclass, fields
import hashlib, json
from typing import Any, Type

from ..types.product_ref import ProductRef


def _canon(obj: Any) -> Any:
    if is_dataclass(obj):
        obj = asdict(obj)
    if isinstance(obj, dict):
        return {k: _canon(v) for k, v in sorted(obj.items(), key=lambda kv: kv[0])}
    if isinstance(obj, (list, tuple)):
        return [_canon(v) for v in obj]
    if isinstance(obj, set):
        return sorted(_canon(v) for v in obj)
    return obj

def compute_algo_id(*, impl_fqcn: str, config: Any | None, salt: str = "algo-v1", n: int = 12) -> str:
    payload = {"impl": impl_fqcn, "config": _canon(config), "salt": salt}
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:n]

def field_names(row_type: Type) -> set[str]:
    if is_dataclass(row_type): return {f.name for f in fields(row_type)}
    # fallback for TypedDict or dict-like rows (declare a __fields__ attr on row_type if you want)
    return set(getattr(row_type, "__fields__", []))

def product_path(prod: ProductRef):
    return "/".join(prod.to_asset_key().path)

def import_symbol(path: str):
    """Import module:attr from a string like 'pkg.mod:ClassOrFunc'."""
    mod, _, attr = path.partition(":")
    if not mod or not attr:
        raise ImportError(f"Invalid import path (expected 'module:attr'): {path}")
    try:
        module = importlib.import_module(mod)
    except ModuleNotFoundError as e:
        raise ImportError(
            f"Module '{mod}' could not be imported while resolving '{path}'. "
            "Make sure the module exists and is on PYTHONPATH."
        ) from e
    try:
        return getattr(module, attr)
    except AttributeError as e:
        raise ImportError(f"{attr} not found in {mod}") from e

