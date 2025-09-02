from __future__ import annotations

import re
from typing import Dict, Mapping, Tuple

from .env import EnvProfile

from .location import FileLocation, TableLocation, Location
from .partition import Partition
from .product_ref import ProductRef


# -------------------------
# Context construction
# -------------------------

def _product_ctx(product: ProductRef) -> Dict[str, str]:
    """
    Tokens you can use in templates:
      {domain}           => path-style, e.g. "raw/dune"
      {domain_path}      => same as {domain}
      {domain_dash}      => dash-joined, e.g. "raw-dune"
      {domain_uscore}    => underscore-joined, e.g. "raw_dune"
      {name}             => product.name
      {version}          => product.version (default handled in ProductRef)
      {product_id}       => fully qualified id if you need it in paths
    """
    dom = tuple(product.domain) if isinstance(product.domain, (list, tuple)) else (product.domain,)
    return {
        "domain": "/".join(dom),
        "domain_path": "/".join(dom),
        "domain_dash": "-".join(dom),
        "domain_uscore": "_".join(dom),
        "name": product.name,
        "version": getattr(product, "version", "v1"),
        "product_id": getattr(product, "id", f"{'-'.join(dom)}.{product.name}:{getattr(product, 'version', 'v1')}"),
    }

def _partition_ctx(partition: Partition | None) -> Dict[str, str]:
    # Partition values are already strings in our design; coerce defensively.
    return {} if partition is None else {k: str(v) for k, v in partition.values.items()}

def _merge_ctx(env: EnvProfile, product: ProductRef, partition: Partition | None) -> Dict[str, str]:
    ctx: Dict[str, str] = {}
    ctx.update({k: str(v) for k, v in (env.vars or {}).items()})
    ctx.update(_product_ctx(product))
    ctx.update(_partition_ctx(partition))
    return ctx

# -------------------------
# Template rendering
# -------------------------

_FIELD_RX = re.compile(r"{(?P<name>[a-zA-Z0-9_]+)}")

def _required_fields(template: str) -> set[str]:
    return {m.group("name") for m in _FIELD_RX.finditer(template)}

def _strict_format(template: str, ctx: Mapping[str, str]) -> str:
    missing = _required_fields(template) - set(ctx.keys())
    if missing:
        raise KeyError(f"Missing fields for template: {sorted(missing)} in '{template}'")
    return template.format(**ctx)

# -------------------------
# Public API
# -------------------------

def resolve_uri_template(
    *,
    template: str,
    env: EnvProfile,
    product: ProductRef,
    partition: Partition | None = None,
) -> str:
    """
    Render any string template using env.vars + product + partition context.
    This is location-agnostic; use for FileLocation.uri_template or anywhere you need templating.
    """
    ctx = _merge_ctx(env, product, partition)
    return _strict_format(template, ctx)

def format_uri(
    *,
    env: EnvProfile,
    product: ProductRef,
    partition: Partition,
    location: FileLocation,
    wildcard: bool = False,
) -> str:
    """
    Resolve a FileLocation into a concrete path. When wildcard=True:
      - if path ends with '/', appends '*.parquet' (or '*.<format>')
      - if path ends with a file, leaves it as-is
    """
    uri = resolve_uri_template(
        template=location.uri_template,
        env=env,
        product=product,
        partition=partition,
    )
    if not wildcard:
        return uri

    # Add a sensible wildcard for directory-style targets
    if uri.endswith("/"):
        ext = "parquet" if location.format == "parquet" else location.format
        return f"{uri}*.{ext}"
    return uri  # looks like a file path already

def resolve_table(
    *,
    env: EnvProfile,
    product: ProductRef,
    location: TableLocation,
) -> Tuple[str, str]:
    """
    Resolve a TableLocation into (database, table_name).
    Table templates often want {domain_uscore}, {name}, {version}.
    """
    # Allow product/env tokens in both database and table templates
    ctx = _merge_ctx(env, product, partition=None)

    # database: "project.dataset" for BQ, or "db" for CH; treat as a literal or template
    database = _strict_format(location.database, ctx) if "{" in location.database else location.database

    table_name = _strict_format(location.table_template, ctx)
    return database, table_name

def compute_storage_id(
    *,
    env: EnvProfile,
    location: Location,
) -> str:
    """
    Stable 'where' identifier used in markers and skip policies.
    Prefer the explicit .storage_id on Location; otherwise synthesize a simple one.
    """
    # First honor explicit storage_id if present
    sid = getattr(location, "storage_id", None)
    if sid:
        return sid

    # Synthesize from obvious cues:
    if isinstance(location, FileLocation):
        uri = location.uri_template
        if uri.startswith("gs://"):
            bucket = env.vars.get("gcs_bucket", "")
            root = env.vars.get("root_prefix", "")
            return f"gcs:{bucket}:{root}"
        if uri.startswith("file://"):
            return "fs:local"
        return "fs:unknown"
    if isinstance(location, TableLocation):
        if location.system == "bigquery":
            return f"bq:{location.database}"
        if location.system == "clickhouse":
            return f"ch:{location.database}"
    return "unknown"
