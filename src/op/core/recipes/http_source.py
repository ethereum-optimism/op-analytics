from dataclasses import is_dataclass, asdict, fields
from datetime import date
from typing import Any, Iterable, Optional, Type

from jinja2 import Environment, DictLoader, FileSystemLoader

from ..interfaces.http_runner import HttpRunner
from ..interfaces.source import Source
from ..types.partition import Partition
from ..utils.helpers import import_symbol


def _env_from_text(text: str) -> Environment:
    return Environment(loader=DictLoader({"_path.j2": text}), autoescape=False)

def _env_from_dir(dir_path: str) -> Environment:
    return Environment(loader=FileSystemLoader(dir_path), autoescape=False)

def _render(env: Environment, name: str, ctx: dict) -> str:
    return env.get_template(name).render(**ctx)

def _dot_get(obj: Any, path: str | None) -> Any:
    if not path:
        return obj
    cur = obj
    for seg in path.split("."):
        if isinstance(cur, dict) and seg in cur:
            cur = cur[seg]
        else:
            return None
    return cur

def _iter_rows(payload: Any, extract_path: str | None) -> Iterable[dict]:
    core = _dot_get(payload, extract_path) if extract_path else payload
    if core is None:
        return []
    if isinstance(core, list):
        for item in core:
            if isinstance(item, dict):
                yield item
            else:
                yield {"value": item}
    elif isinstance(core, dict):
        yield core
    else:
        yield {"value": core}

def _coerce_row(row: dict, row_type: Type) -> dict:
    if is_dataclass(row_type):
        flds = fields(row_type)                      # dataclass fields
        names = {f.name for f in flds}
        # Include only declared fields (except extras for now)
        values = {k: row.get(k) for k in names if k != "extras"}
        # If your dataclass declares `extras: Dict[str, Any]`, preserve unknowns
        if "extras" in names:
            values["extras"] = {k: v for k, v in row.items() if k not in names}
        try:
            return asdict(row_type(**values))
        except TypeError as e:
            # Surface a helpful error with visible keys
            raise TypeError(
                f"Could not coerce row to {row_type.__name__}: {e}; "
                f"row keys: {list(row.keys())}"
            ) from e
    return row


class HttpJsonSource(Source[dict]):
    """
    Jinja-templated HTTP JSON source with typed request/response, mock data, and simple pagination.
    """
    def __init__(
        self,
        runner: Optional[HttpRunner],
        base_url: str,

        path_template: str | None = None,  # inline template (text)
        path_template_dir: str | None = None,  # directory with 'path.j2'
        path_template_name: str = "path.j2",
        method: str = "GET",
        timeout_sec: float | None = 30.0,
        request_cls_path: str | None = None,  # e.g., "...config:Request"
        row_type_path: str = None,  # e.g., "...schema:Record"
        headers_defaults: dict[str, str] = {},
        defaults: dict = {},
        anchor_from_partition_key: str = "dt",
        extract_path: str | None = None,  # dotted path to rows inside JSON
        next_url_json_path: str | None = None, # dotted path to next page url
        cursor_json_path: str | None = None,  # dotted path to cursor token
        cursor_param_name: str | None = None,  # query param name for cursor
        max_pages: int = 100,
        use_mock_data: bool = False,
        mock_data_fn_path: str | None = None,
    ):
        # TODO: this is overkill for injection params, need to simplify this or trap it all in a config object
        #   or a config factory
        self.runner = runner
        self.base_url = base_url
        self.path_template = path_template
        self.path_template_dir = path_template_dir
        self.path_template_name = path_template_name
        self.method = method
        self.timeout_sec = timeout_sec
        self.request_cls_path = request_cls_path
        self.row_type_path = row_type_path
        self.use_mock_data = use_mock_data
        self.mock_data_fn_path = mock_data_fn_path
        self.headers_defaults = headers_defaults
        self.defaults = defaults
        self.anchor_from_partition_key = anchor_from_partition_key
        self.extract_path = extract_path
        self.next_url_json_path = next_url_json_path
        self.cursor_json_path = cursor_json_path
        self.cursor_param_name = cursor_param_name
        self.max_pages = max_pages


    def fetch(self, partition: Partition) -> Iterable[dict]:
        # Resolve typed classes
        RequestCls = import_symbol(self.request_cls_path) if self.request_cls_path else None
        RowType = import_symbol(self.row_type_path)
        mock_fn = import_symbol(self.mock_data_fn_path) if (self.use_mock_data and self.mock_data_fn_path) else None

        # Build request config (anchor optional)
        anchor_s = partition.values.get(self.anchor_from_partition_key)
        anchor   = date.fromisoformat(anchor_s) if anchor_s else self.defaults.get("anchor_day")

        req = (
            RequestCls(
                anchor_day=anchor,
                **{k: v for k, v in self.defaults.items() if k != "anchor_day"}
            ) if RequestCls else None
        )
        params = req.to_query_params(anchor=anchor) if req and hasattr(req, "to_query_params") else dict(self.defaults)
        headers = dict(self.headers_defaults)
        if req and hasattr(req, "to_headers"):
            try:
                headers.update(req.to_headers(anchor=anchor))
            except Exception:
                pass
        body = None
        if self.method.upper() != "GET" and req and hasattr(req, "to_body"):
            body = req.to_body(anchor=anchor)

        # mock path
        if self.use_mock_data:
            if mock_fn:
                for r in mock_fn(req) if RequestCls else mock_fn(None):
                    yield _coerce_row(r, RowType)
            return

        if self.runner is None:
            raise RuntimeError("HttpJsonSource runner is not configured. Provide a HttpRunner or set use_mock_data=True.")

        # Render path
        if self.path_template:
            env = _env_from_text(self.path_template)
            path = _render(env, "_path.j2", {**params, "req": req})
        elif self.path_template_dir:
            env = _env_from_dir(self.path_template_dir)
            path = _render(env, self.path_template_name, {**params, "req": req})
        else:
            raise ValueError("Either path_template or path_template_dir must be provided.")

        url = f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"

        # Fetch pages
        pages = 0
        next_url: str | None = url
        cursor: str | None = None

        while next_url and pages < self.max_pages:
            payload = self.runner.request_json(
                self.method,
                next_url,
                headers=headers,
                params=(
                    params if cursor is None
                    else {**params, self.cursor_param_name: cursor} if self.cursor_param_name
                    else params
                ),
                json=body,
                timeout=self.timeout_sec
            )

            for row in _iter_rows(payload, self.extract_path):
                yield _coerce_row(row, RowType)

            pages += 1
            # Pagination by next_url or cursor
            if self.next_url_json_path:
                next_url = _dot_get(payload, self.next_url_json_path)
            elif self.cursor_json_path and self.cursor_param_name:
                cursor = _dot_get(payload, self.cursor_json_path)
                next_url = url if cursor else None
            else:
                next_url = None