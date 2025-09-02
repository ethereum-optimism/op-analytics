import importlib
from pathlib import Path

from ..configs.bindings import SqlSourceConfig
from ...core.defs.product_contract import ProductContract
from ...core.defs.resource_binding import ResourceBinding
from ...core.defs.source_binding import SourceBinding
from ...core.interfaces.source import Source
from ...core.recipes.sql_source import SqlSource
from ...core.utils.helpers import import_symbol



def _qual(pkg: str, spec: str) -> str:
    if ":" not in spec:
        raise ValueError(f"Expected '<module>:<attr>', got '{spec}'")
    mod, attr = spec.split(":", 1)
    return f"{pkg}.{mod}:{attr}"

def _default_key(recipe_pkg: str) -> str:
    parts = recipe_pkg.split(".")
    provider = parts[-2] if len(parts) >= 2 else parts[-1]
    name = parts[-1]
    return f"src_{provider}_{name}_sql"

def bind_sql_source(cfg: SqlSourceConfig) -> SourceBinding:
    pkg = importlib.import_module(cfg.recipe_pkg)
    pkg_dir = Path(pkg.__file__).parent
    templates_dir = pkg_dir / "templates"
    tmpl_path = templates_dir / cfg.main_template
    if not templates_dir.exists() or not tmpl_path.exists():
        raise FileNotFoundError(f"SQL template not found: {tmpl_path}")

    row_type = import_symbol(_qual(cfg.recipe_pkg, cfg.row_type_spec))
    request_cls_path = _qual(cfg.recipe_pkg, cfg.request_spec)
    mock_fn_path = _qual(cfg.recipe_pkg, cfg.mock_data_spec) if (cfg.mock_data_spec and cfg.use_mock_data) else None

    src = SqlSource(
        runner=cfg.runner,
        templates_dir=str(templates_dir),
        main_template=cfg.main_template,
        request_cls_path=request_cls_path,
        row_type_path=_qual(cfg.recipe_pkg, cfg.row_type_spec),
        defaults=cfg.defaults,
        use_mock_data=cfg.use_mock_data,
        mock_data_fn_path=mock_fn_path,
        anchor_from_partition_key=cfg.anchor_from_partition_key,
    )

    rb = ResourceBinding(key=_default_key(cfg.recipe_pkg), resource=src, expected=Source)
    # Your SourceBinding, carrying row_type for convenience
    return SourceBinding(product=cfg.product, source=rb, name=cfg.product.name, row_type=row_type, meta=None)

def contract_from_source(sb: SourceBinding, *, required: tuple[str, ...] = ()) -> ProductContract:
    if sb.row_type is None:
        raise ValueError(f"SourceBinding {sb.name} has no row_type set.")
    return ProductContract(product=sb.product, row_type=sb.row_type, required_fields=required)
