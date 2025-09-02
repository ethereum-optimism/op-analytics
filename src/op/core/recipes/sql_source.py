from dataclasses import is_dataclass
from datetime import date
from typing import Iterable, Any, Type, Optional

from jinja2 import Environment, FileSystemLoader

from ..interfaces.source import Source
from ..interfaces.sql_runner import SqlRunner
from ..types.partition import Partition
from ..utils.helpers import import_symbol


def _env(templates_dir: str):
    env = Environment(loader=FileSystemLoader(templates_dir), autoescape=False)
    def sqlquote(v: Any) -> str:
        if v is None: return "NULL"
        if isinstance(v, (int, float)): return str(v)
        s = str(v).replace("'", "''")
        return f"'{s}'"
    env.filters["sqlquote"] = sqlquote
    return env

def _coerce_row(row: dict, row_type: Type):
    return row_type(**row) if is_dataclass(row_type) else row

class SqlSource(Source[dict]):
    def __init__(
        self,
        runner: Optional[SqlRunner],
        templates_dir: str,
        main_template: str,
        request_cls_path: str,
        row_type_path: str,
        use_mock_data: bool,
        mock_data_fn_path: Optional[str],
        anchor_from_partition_key: str,
        defaults: dict
    ):
        self.runner = runner
        self.templates_dir = templates_dir
        self.main_template = main_template
        self.request_cls_path = request_cls_path
        self.row_type_path = row_type_path
        self.use_mock_data = use_mock_data
        self.mock_data_fn_path = mock_data_fn_path
        self.anchor_from_partition_key = anchor_from_partition_key
        self.defaults = defaults

    def fetch(self, partition: Partition) -> Iterable[dict]:
        RequestCls = import_symbol(self.request_cls_path)
        RowType = import_symbol(self.row_type_path)
        mock_data_fn = import_symbol(self.mock_data_fn_path) if self.mock_data_fn_path else None

        anchor_s = partition.values.get(self.anchor_from_partition_key)
        anchor = date.fromisoformat(anchor_s) if anchor_s else self.defaults.get("anchor_day", date.today())
        req = RequestCls(anchor_day=anchor, **{k:v for k,v in self.defaults.items() if k!="anchor_day"})
        params = req.to_query_params(anchor=anchor) if hasattr(req, "to_query_params") else {}

        if self.use_mock_data and mock_data_fn:
            for r in mock_data_fn(req):
                yield _coerce_row(r, RowType)
            return
        if self.use_mock_data:
            return

        if self.runner is None:
            raise RuntimeError(
                "SqlRunner runner is not configured. Construct this class with a SqlRunner or set use_mock_data=True."
            )

        env = _env(self.templates_dir)
        sql = env.get_template(self.main_template).render(**params)
        for r in self.runner.run_sql(sql):
            yield _coerce_row(r, RowType)
