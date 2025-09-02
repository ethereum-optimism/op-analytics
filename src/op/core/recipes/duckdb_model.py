from dataclasses import dataclass, asdict, is_dataclass
from typing import Dict, Any, List

import duckdb
import pandas as pd
from jinja2 import Environment, FileSystemLoader

from ..defs.node_contracts import NodeContracts
from ..interfaces.node import Node
from ..defs.contract_aware import ContractAware
from ..types.dataset import Dataset
from ..types.partition import Partition
from ..types.product_ref import ProductRef


def _render_sql(*, templates_dir: str, template_name: str, ctx: Dict[str, Any]) -> str:
    env = Environment(loader=FileSystemLoader(templates_dir), autoescape=False)
    return env.get_template(template_name).render(**ctx)

@dataclass(frozen=True)
class DuckDBSqlModelNode(ContractAware, Node[Dict[ProductRef, Dataset], Dict[ProductRef, Dataset]]):
    """
    Generic model runner:
      - expects named input ports (e.g., 'txs', 'gas', 'fees')
      - registers each input dataset as a DuckDB temp table with the SAME alias
      - renders a Jinja2 SQL template with {alias}_table variables and a 'dt' param
      - executes SQL and maps the result rows to dicts or dataclasses
    """
    templates_dir: str
    template_name: str = "query.sql.j2"
    out_port: str = "out"
    dt_param_name: str = "dt"
    _contracts: NodeContracts | None = None  # injected by binder

    @property
    def contracts(self) -> NodeContracts:
        c = self._contracts or getattr(self, "__node_contracts__", None)
        if c is None:
            raise RuntimeError(f"{self.__class__.__name__} has no contracts attached")
        return c

    def execute(self, inputs: Dict[ProductRef, Dataset], part: Partition) -> Dict[ProductRef, Dataset]:
        # 1) Build pandas DataFrames for each required port and track alias names
        alias_to_df: Dict[str, pd.DataFrame] = {}
        for port in self.contracts.requires:         # ← property access
            alias = port.name
            ds = self.req_ds(inputs, alias)
            alias_to_df[alias] = pd.DataFrame(ds.rows)

        # 2) Build Jinja context: expose "<alias>_table" + optional 'dt'
        ctx: Dict[str, Any] = {f"{alias}_table": alias for alias in alias_to_df.keys()}
        if self.dt_param_name:
            ctx[self.dt_param_name] = part.values.get(self.dt_param_name)

        # 3) Render + execute
        sql = _render_sql(templates_dir=self.templates_dir, template_name=self.template_name, ctx=ctx)
        con = duckdb.connect()
        try:
            for alias, df in alias_to_df.items():
                con.register(alias, df)
            out_df: pd.DataFrame = con.execute(sql).fetchdf()
        finally:
            con.close()

        # 4) Coerce rows to dicts (or dataclass if you prefer)
        rows: List[Dict[str, Any]] = out_df.to_dict(orient="records")
        provided_pc = next(p for p in self.contracts.provides if p.name == self.out_port)
        RowType = provided_pc.contract.row_type
        if is_dataclass(RowType):
            rows = [asdict(RowType(**r)) for r in rows]

        return self.out_map(rows, self.out_port)
