import importlib
from pathlib import Path

from ..configs.bindings import HttpSourceConfig
from ...core.defs.resource_binding import ResourceBinding
from ...core.defs.source_binding import SourceBinding
from ...core.interfaces.source import Source as ISource
from ...core.recipes.http_source import HttpJsonSource
from ...platform.utils.helpers import make_key


def _qual(pkg: str, spec: str) -> str:
    # "<module>:<attr>" under recipe pkg
    mod, attr = spec.split(":", 1)
    return f"{pkg}.{mod}:{attr}"

def _default_key(recipe_pkg: str) -> str:
    parts = recipe_pkg.split(".")
    provider = parts[-2] if len(parts) >= 2 else parts[-1]
    name = parts[-1]
    return make_key("src", provider, name, "http")

def bind_http_source(cfg: HttpSourceConfig) -> SourceBinding[HttpJsonSource]:
    """
    Convention-based HTTP JSON source binder:
      - recipe_pkg/
          config.py   (optional Request dataclass with to_query_params/to_headers/to_body)
          schema.py   (required Response @dataclass Record)
          mock_data.py (optional generate(req) -> Iterable[dict])
          templates/
            path.j2   (path template; optional if you pass inline path_template_text)
    """
    cfg.validate()

    pkg = importlib.import_module(cfg.recipe_pkg)
    tdir = Path(pkg.__file__).parent / "templates"

    # Inline text vs file-based template
    path_template_text = getattr(cfg, "path_template_text", None)
    path_template_dir  = None
    path_template_name = "path.j2"
    if path_template_text is None:
        path_template_dir  = str(tdir)
        path_template_name = cfg.path_template_name or "path.j2"

    src = HttpJsonSource(
        runner=cfg.runner,
        base_url=cfg.base_url,

        # templating (either text OR dir+name)
        path_template=path_template_text,
        path_template_dir=path_template_dir,
        path_template_name=path_template_name,

        method=cfg.method or "GET",
        timeout_sec=cfg.timeout_sec or 30.0,

        request_cls_path=_qual(cfg.recipe_pkg, cfg.request_spec) if cfg.request_spec else None,
        row_type_path=_qual(cfg.recipe_pkg, cfg.row_type_spec),

        headers_defaults=dict(getattr(cfg, "headers_defaults", {}) or {}),
        defaults=dict(cfg.defaults or {}),
        anchor_from_partition_key=cfg.anchor_from_partition_key or "dt",

        extract_path=cfg.extract_path,                     # e.g., "data.items"
        next_url_json_path=cfg.next_url_json_path,        # e.g., "next"
        cursor_json_path=cfg.cursor_json_path,            # e.g., "cursor"
        cursor_param_name=cfg.cursor_param_name,          # e.g., "cursor"
        max_pages=cfg.max_pages or 100,

        use_mock_data=bool(cfg.use_mock_data),
        mock_data_fn_path=_qual(cfg.recipe_pkg, cfg.mock_data_spec) if (cfg.mock_data_spec and cfg.use_mock_data) else None,
    )

    rb = ResourceBinding(key=_default_key(cfg.recipe_pkg), resource=src, expected=ISource)
    return SourceBinding(product=cfg.product, source=rb, name=cfg.product.name, row_type=None, meta=None)
