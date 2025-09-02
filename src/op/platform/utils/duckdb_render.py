from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Mapping

from jinja2 import Environment, FileSystemLoader, BaseLoader, StrictUndefined

from ...core.types.partition import Partition
from ...core.types.product_ref import ProductRef


def _canon(obj):
    if is_dataclass(obj): obj = asdict(obj)
    if isinstance(obj, dict): return {k:_canon(v) for k,v in sorted(obj.items())}
    if isinstance(obj, (list, tuple, set)): return [ _canon(v) for v in obj ]
    return obj

def resolve_uri_template(prod: ProductRef, part: Partition, wildcard: bool=False) -> str:
    """
    Compute a path (gs://... or file://...) from product.locator["uri_template"].
    Template can use {{domain}}, {{name}}, {{version}}, {{dt}}, etc.
    """
    loc = getattr(prod, "locator", None) or {}
    tmpl: str | None = loc.get("uri_template")
    if not tmpl:
        raise ValueError(f"Product {prod.id} is missing locator.uri_template")
    ctx = {
        "domain": "-".join(prod.domain),
        "name": prod.name,
        "version": getattr(prod, "version", "v1"),
        **(part.values or {}),
    }
    s = tmpl.format(**ctx)
    if wildcard and "*" not in s and s.endswith("/"):
        # Helpful default for inputs: add wildcard for files when reading
        s = s + "*.parquet"
    return s

def render_sql(
    *, sql_text: str | None, sql_template_path: str | None, context: Mapping[str, object]
) -> str:
    if sql_text:
        env = Environment(loader=BaseLoader(), undefined=StrictUndefined)
        return env.from_string(sql_text).render(**context)
    # else template file
    path = Path(sql_template_path)  # type: ignore[arg-type]
    env = Environment(loader=FileSystemLoader(path.parent), undefined=StrictUndefined)
    return env.get_template(path.name).render(**context)
