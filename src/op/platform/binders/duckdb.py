import importlib
from dataclasses import dataclass
from typing import Dict

import duckdb
from jinja2 import Environment, FileSystemLoader

from ..configs.duckdb import DuckDBModelConfig
from ...core.defs.node_binding import NodeBinding
from ...core.defs.product_contract import ProductContract
from ...core.interfaces.node import Node
from ...core.types.dataset import Dataset
from ...core.types.partition import Partition
from ...core.types.product_ref import ProductRef

# NEW: import EnvProfile and format_uri
from ...core.types.env import EnvProfile
from ...core.types.uri import format_uri

# You may already have these; otherwise implement them to return FileLocation objects
# for the given ProductRef(s). This is just an interface hint:
from ...core.types.location import FileLocation  # ensure this exists in your codebase

def get_location_for_product(product: ProductRef) -> FileLocation:
    """
    Resolve the default FileLocation for a product.
    Replace this body with your registry/lookup.
    """
    # e.g., return registry.locations[product]
    raise NotImplementedError("Implement product->location lookup")

@dataclass(frozen=True)
class DuckDBSqlModel(Node[Dict[ProductRef, Dataset], Dict[ProductRef, Dataset]]):
    cfg: DuckDBModelConfig
    env: EnvProfile  # <-- thread env in

    def execute(self, _inputs: Dict[ProductRef, Dataset], part: Partition) -> Dict[ProductRef, Dataset]:
        cfg = self.cfg
        cfg.validate()

        con = duckdb.connect()
        try:
            # Session bootstrap
            if cfg.session.enable_httpfs:
                con.execute("INSTALL httpfs; LOAD httpfs;")
            if cfg.session.extra_sql:
                for sql in cfg.session.extra_sql:
                    con.execute(sql)
            if cfg.session.gcs:
                con.execute(
                    "CREATE SECRET (TYPE gcs, KEY_ID ?, SECRET ?);",
                    [cfg.session.gcs.key_id, cfg.session.gcs.secret],
                )
            if cfg.session.udf_modules:
                for mod_path in cfg.session.udf_modules:
                    mod = importlib.import_module(mod_path)
                    if not hasattr(mod, "register_udfs"):
                        raise RuntimeError(f"UDF module {mod_path} missing register_udfs(con)")
                    mod.register_udfs(con)  # type: ignore[attr-defined]

            # ---------- Helpers for Jinja context ----------
            def input_path(alias: str) -> str:
                prod = cfg.inputs[alias]
                in_loc = get_location_for_product(prod)
                return format_uri(
                    env=self.env,
                    product=prod,
                    partition=part,
                    location=in_loc,
                    wildcard=True,   # glob for read_parquet
                )

            def read_parquet_expr(alias: str) -> str:
                return f"read_parquet('{input_path(alias)}')"

            # ---------- Output path (no wildcard) ----------
            out_loc = get_location_for_product(cfg.output)
            out_path = format_uri(
                env=self.env,
                product=cfg.output,
                partition=part,
                location=out_loc,
                wildcard=False,
            )
            if cfg.output_filename:
                out_path = (out_path.rstrip("/") + "/" + cfg.output_filename)

            ctx = {
                "input_path": input_path,
                "read_parquet": read_parquet_expr,
                "output_path": out_path,
                **(cfg.template_context or {}),
            }

            # Render SQL
            if cfg.sql_text:
                sql = Environment().from_string(cfg.sql_text).render(**ctx)
            else:
                template_pkg = cfg.template_pkg  # e.g., "op.catalog.models.agg_duckdb"
                template_name = cfg.sql_template_name or "query.sql.j2"
                mod = importlib.import_module(template_pkg)
                env_j2 = Environment(loader=FileSystemLoader(str(getattr(mod, "__path__")[0])), autoescape=False)
                sql = env_j2.get_template(template_name).render(**ctx)

            fmt = cfg.format.lower()
            ovw = " OVERWRITE" if cfg.overwrite else ""
            con.execute(f"COPY ({sql}) TO '{out_path}' (FORMAT '{fmt}'{ovw});")

            return {cfg.output: Dataset(rows=[{"path": out_path}])}
        finally:
            con.close()


def bind_duckdb_model(cfg: DuckDBModelConfig, env: EnvProfile) -> NodeBinding:
    """Config-first binder; builds a single-output DuckDB SQL model NodeBinding."""
    reqs = tuple(ProductContract(product=p, row_type=dict) for p in cfg.inputs.values())
    provs = (ProductContract(product=cfg.output, row_type=dict, required_fields=tuple(cfg.required_output_fields or ())),)
    node = DuckDBSqlModel(cfg=cfg, env=env)  # <-- pass env in
    return NodeBinding(node=node, requires=reqs, provides=provs)
