from dataclasses import dataclass, asdict, is_dataclass, fields
from datetime import datetime, date
from typing import Dict, Any, List, Callable, Optional

import polars as pl

from ..defs.contract_aware import ContractAware
from ..defs.frame import Frame
from ..defs.node_contracts import NodeContracts
from ..defs.source_meta import SourceMeta
from ..interfaces.node import Node
from ..types.dataset import Dataset
from ..types.partition import Partition
from ..types.product_ref import ProductRef
from ..utils.helpers import import_symbol


# --- NEW: helpers ------------------------------------------------------------

# Canonicalize a row so every value is scalar-ish.
def _canon_row(r: Any) -> Dict[str, Any]:
    """
    Dataclass→dict; normalize scalars; serialize non-scalars to JSON strings.
    Ensures values are among {None, str, int, float, bool} so the DF builder
    doesn't see nested types.
    """
    if is_dataclass(r):
        r = asdict(r)
    elif hasattr(r, "__dict__") and not isinstance(r, dict):
        r = dict(r.__dict__)
    out: Dict[str, Any] = {}
    for k, v in (r or {}).items():
        if isinstance(v, (date, datetime)):
            out[k] = v.date().isoformat() if isinstance(v, datetime) else v.isoformat()
        elif isinstance(v, (str, int, float, bool)) or v is None:
            out[k] = v
        else:
            # lists, dicts, sets, tuples, etc. → JSON string
            try:
                out[k] = jsonlib.dumps(v, separators=(",", ":"))
            except Exception:
                out[k] = str(v)
    return out

def _harmonize_mixed_columns(canon: List[Dict[str, Any]]) -> None:
    """
    In-place: for any column that has BOTH strings and non-strings across rows,
    coerce ALL non-string values to string. This avoids mixed-type failures
    like {"blockchain": "ethereum"} vs {"blockchain": 10}.
    We do NOT coerce numeric-only mixes (int+float) since Polars can unify those.
    """
    # Collect type set per column (excluding None)
    col_types: Dict[str, set[type]] = {}
    for r in canon:
        for k, v in r.items():
            if v is None:
                continue
            t = type(v)
            col_types.setdefault(k, set()).add(t)

    # Columns that include strings AND some other type
    to_string: List[str] = []
    for k, types in col_types.items():
        if str in types and (types - {str}):
            to_string.append(k)

    if not to_string:
        return

    # Coerce non-strings in those columns to str
    for r in canon:
        for k in to_string:
            v = r.get(k)
            if v is not None and not isinstance(v, str):
                r[k] = str(v)

# Known date-like columns to auto-cast from Utf8→Date
_DATE_COL_GUESSES = {"dt", "dt_day"}

def _rows_to_pl_df(rows: List[Any]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame()

    canon = [_canon_row(r) for r in rows]
    _harmonize_mixed_columns(canon)  # <-- NEW: prevent mixed string/non-string per column
    print(canon)
    # Scan all rows to infer stable schema
    df = pl.from_dicts(canon)

    # Cast common date-like columns from Utf8 → Date if present
    dateish = [c for c in df.columns if c in _DATE_COL_GUESSES or c.endswith("_date")]
    if dateish:
        df = df.with_columns([
            pl.col(c).str.strptime(pl.Date, strict=False, exact=False).alias(c)
            for c in dateish
            if df.schema.get(c) == pl.Utf8
        ])
    return df
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class PolarsModuleModelNode(
    ContractAware,
    Node[Dict[ProductRef, Dataset], Dict[ProductRef, Dataset]],
):
    func_spec: str                     # "a.b.c:transform"
    out_port: str = "out"
    dt_param_name: str = "dt"
    config: Optional[dict] = None
    _contracts: Optional[NodeContracts] = None

    @property
    def contracts(self) -> NodeContracts:
        c = self._contracts or getattr(self, "__node_contracts__", None)
        if c is None:
            raise RuntimeError(f"{self.__class__.__name__} has no contracts attached")
        return c

    def require_source_binding(self, alias: str):
        m = getattr(self, "__alias_to_source_binding__", None)
        if isinstance(m, dict) and alias in m:
            return m[alias]
        raise KeyError(f"No SourceBinding recorded for alias '{alias}'")

    def _frames_for_inputs(self, inputs: Dict[ProductRef, Dataset]) -> Dict[str, Frame]:
        frames: Dict[str, Frame] = {}
        alias_map = getattr(self, "__alias_to_source_binding__", None)
        casts_map = getattr(self, "__input_casts_by_alias__", {})

        for port in self.contracts.requires:
            alias = port.name
            ds = self.req_ds(inputs, alias)
            if isinstance(alias_map, dict) and alias in alias_map:
                sb = alias_map[alias]
                meta = sb.meta or SourceMeta(kind="sql", provider="unknown", name=alias, details={})
                prod = sb.product
            else:
                meta = SourceMeta(kind="unknown", provider="unknown", name=alias, details={})
                prod = port.contract.product

            df = _rows_to_pl_df(list(ds.rows))

            wanted = casts_map.get(alias, {})
            if wanted:
                df = df.with_columns([
                    pl.col(col).cast(dtype, strict=False).alias(col)
                    for col, dtype in wanted.items()
                    if col in df.columns
                ])

            frames[alias] = Frame(df=df, product=prod, meta=meta)
        return frames

    def execute(self, inputs: Dict[ProductRef, Dataset], part: Partition) -> Dict[ProductRef, Dataset]:
        alias_to_frame = self._frames_for_inputs(inputs)
        transform_fn: Callable[..., pl.DataFrame] = import_symbol(self.func_spec)

        kwargs: Dict[str, Any] = {"inputs": alias_to_frame}
        if self.dt_param_name:
            kwargs["dt"] = part.values.get(self.dt_param_name)
        if self.config is not None:
            kwargs["config"] = self.config

        out_df = transform_fn(**kwargs)
        if not isinstance(out_df, pl.DataFrame):
            raise TypeError(f"Transform {self.func_spec} must return polars.DataFrame, got {type(out_df)}")

        provided = next(p for p in self.contracts.provides if p.name == self.out_port)
        rows = out_df.to_dicts()

        # optional: if your output row type is a dataclass, coerce as you do elsewhere
        RowType = provided.contract.row_type
        if is_dataclass(RowType):
            names = {f.name for f in fields(RowType)}
            out: List[Dict[str, Any]] = []
            for r in rows:
                vals = {k: r.get(k) for k in names if k != "extras"}
                if "extras" in names:
                    vals["extras"] = {k: v for k, v in r.items() if k not in names}
                out.append(asdict(RowType(**vals)))
            rows = out

        return self.out_map(rows, self.out_port)
