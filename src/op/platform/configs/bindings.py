from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Mapping, Sequence, Dict

from op.core.defs.node_binding import NodeBinding
from op.core.defs.source_binding import SourceBinding
from op.core.interfaces.product_io import ProductIO
from op.core.interfaces.sql_runner import SqlRunner        # protocol: run_sql(sql) -> Iterable[dict]
from op.core.interfaces.http_runner import HttpRunner      # protocol: request_json(method, url, ...) -> Any
from op.core.types.product_ref import ProductRef


# -----------------------------
# SQL recipe source (Jinja SQL)
# -----------------------------

@dataclass(frozen=True)
class SqlSourceConfig:
    """
    Config for bind_sql_source():
      - recipe_pkg:
          <pkg>/
            config.py          (optional dataclass Config with .to_query_params(anchor))
            schema.py          (required: @dataclass Record)
            mock_data.py       (optional: generate(cfg) -> Iterable[dict])
            templates/query.sql.j2 (default template name)
    """
    product: ProductRef
    recipe_pkg: str
    runner: Optional[SqlRunner] = None              # required if use_mock_data=False
    use_mock_data: bool = False
    defaults: dict = field(default_factory=dict)
    main_template: str = "query.sql.j2"
    request_spec: str = "config:Config"             # "<module>:<attr>" under recipe_pkg
    row_type_spec: str = "schema:Record"
    mock_data_spec: Optional[str] = "mock_data:generate"
    anchor_from_partition_key: str = "dt"

    def validate(self) -> None:
        from importlib import import_module
        pkg = import_module(self.recipe_pkg)
        tdir = Path(pkg.__file__).parent / "templates"
        if not tdir.exists():
            raise FileNotFoundError(f"[SqlSourceConfig] templates dir not found: {tdir}")
        tfile = tdir / self.main_template
        if not tfile.exists():
            raise FileNotFoundError(f"[SqlSourceConfig] template file not found: {tfile}")
        if not self.use_mock_data and self.runner is None:
            raise ValueError("[SqlSourceConfig] runner is required when use_mock_data=False")


# -----------------------------
# HTTP JSON source (Jinja path)
# -----------------------------

@dataclass(frozen=True)
class HttpSourceConfig:
    """
    Config for bind_http_source():
      - recipe_pkg:
          <pkg>/
            schema.py         (required: @dataclass Record)
            mock_data.py      (optional: generate(req) -> Iterable[dict])
            templates/path.j2 (optional if you provide path_template_text)
      - Either supply path_template_text OR a file name via path_template_name.
      - Request dataclass (optional) can produce params/headers/body:
          config.py: class Config(...):
              .to_query_params(anchor) -> dict
              .to_headers(anchor) -> dict
              .to_body(anchor) -> dict | list | None
    """
    product: ProductRef
    recipe_pkg: str

    runner: Optional[HttpRunner] = None              # required if use_mock_data=False
    base_url: str = ""                               # e.g., "https://api.llama.fi"
    method: str = "GET"
    timeout_sec: float = 30.0

    # Path templating: choose ONE of the two
    path_template_name: Optional[str] = "path.j2"    # in recipe_pkg/templates/
    path_template_text: Optional[str] = None         # inline Jinja

    # Optional request/response typing
    request_spec: Optional[str] = "config:Config"    # "<module>:<attr>" or None
    row_type_spec: str = "schema:Record"

    # Mocks
    use_mock_data: bool = False
    mock_data_spec: Optional[str] = "mock_data:generate"

    # Extra defaults passed into request templates
    defaults: dict = field(default_factory=dict)

    # JSON extraction/pagination (dotted paths)
    extract_path: Optional[str] = None               # e.g., "data.items"
    next_url_json_path: Optional[str] = None         # e.g., "next"
    cursor_json_path: Optional[str] = None           # e.g., "cursor"
    cursor_param_name: Optional[str] = None          # e.g., "cursor"
    max_pages: int = 100

    # Partition anchor mapping
    anchor_from_partition_key: str = "dt"

    # Headers baseline
    headers_defaults: Mapping[str, str] = field(default_factory=dict)

    def validate(self) -> None:
        if not self.base_url:
            raise ValueError("[HttpSourceConfig] base_url is required")
        if not self.use_mock_data and self.runner is None:
            raise ValueError("[HttpSourceConfig] runner is required when use_mock_data=False")
        if self.path_template_text:
            return  # inline path provided, all good
        # Otherwise, we expect templates/path.j2 (or custom name) to exist
        from importlib import import_module
        pkg = import_module(self.recipe_pkg)
        tdir = Path(pkg.__file__).parent / "templates"
        if not tdir.exists():
            raise FileNotFoundError(f"[HttpSourceConfig] templates dir not found: {tdir}")
        name = self.path_template_name or "path.j2"
        pfile = tdir / name
        if not pfile.exists():
            raise FileNotFoundError(f"[HttpSourceConfig] path template not found: {pfile}")


# -----------------------------------
# Python function "source" (optional)
# -----------------------------------

@dataclass(frozen=True)
class PythonSourceConfig:
    """
    Optional: for a pure Python source binder (if/when you add it).
    Not used by the current examples/binders, but kept for parity with earlier drafts.
    """
    product: ProductRef
    func_path: str                          # "package.module:function"
    kwargs: dict = field(default_factory=dict)
    use_mock_data: bool = False
    mock_func_path: Optional[str] = None
    anchor_from_partition_key: str = "dt"

    def validate(self) -> None:
        if not self.use_mock_data and not self.func_path:
            raise ValueError("[PythonSourceConfig] func_path required when use_mock_data=False")


@dataclass(frozen=True)
class PolarsModelConfig:
    """
    Config for bind_polars_model():
      - func_spec: "pkg.module:transform"
        transform(df_map: dict[str, pl.DataFrame], **kwargs) -> dict[str, pl.DataFrame]
      - Single-output models set out_port to the desired key of returned dict.
    """
    name: str
    func_spec: str
    inputs: Dict[str, ProductRef]      # alias -> ProductRef
    output: ProductRef
    out_port: str                      # which key in returned df_map to persist
    dt_param_name: Optional[str] = "dt"

    # Optional casts for predictable typing (per input alias)
    # e.g., {"txs": {"chain_id": pl.Int64, "dt": pl.Utf8}}
    input_casts: Optional[dict[str, dict[str, object]]] = None

    # Optional contract hint for required fields of the output
    required_output_fields: Optional[tuple[str, ...]] = None

    def validate(self) -> None:
        if not self.name:
            raise ValueError("[PolarsModelConfig] name is required")
        if not self.func_spec or ":" not in self.func_spec:
            raise ValueError("[PolarsModelConfig] func_spec must be 'pkg.module:func'")
        if not self.inputs:
            raise ValueError("[PolarsModelConfig] at least one input is required")
        if not self.out_port:
            raise ValueError("[PolarsModelConfig] out_port is required")
# ---------- Pipeline config ----------

@dataclass(frozen=True)
class PipelineConfig:
    sources: Sequence[SourceBinding]      # filled by binders
    nodes: Sequence[NodeBinding]          # created via bind_node(...)
    default_io: ProductIO                   # used for all produced products unless overridden
    io_overrides: Mapping[ProductRef, ProductIO] = field(default_factory=dict)

    def validate(self) -> None:
        if not self.sources and not self.nodes:
            raise ValueError("[PipelineConfig] must include at least one source or node")
