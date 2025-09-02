import importlib
from dataclasses import dataclass
from typing import Dict

import polars as pl

from ..configs.polars import PolarsModelConfig
from ...core.defs.node_binding import NodeBinding
from ...core.defs.product_contract import ProductContract
from ...core.interfaces.node import Node
from ...core.types.dataset import Dataset
from ...core.types.partition import Partition
from ...core.types.product_ref import ProductRef


@dataclass(frozen=True)
class PolarsModuleModelNode(Node[Dict[ProductRef, Dataset], Dict[ProductRef, Dataset]]):
    cfg: PolarsModelConfig

    def execute(self, inputs: Dict[ProductRef, Dataset], part: Partition) -> Dict[ProductRef, Dataset]:
        self.cfg.validate()
        mod_name, func_name = self.cfg.func_spec.split(":", 1)
        mod = importlib.import_module(mod_name)
        fn = getattr(mod, func_name)

        # materialize input DataFrames by alias
        df_map: Dict[str, pl.DataFrame] = {}
        for alias, prod in self.cfg.inputs.items():
            rows = inputs[prod].rows
            df_map[alias] = pl.DataFrame(rows) if rows else pl.DataFrame()

        # Optional casts
        if self.cfg.input_casts:
            for alias, casts in self.cfg.input_casts.items():
                if alias in df_map:
                    for col, dtype in casts.items():
                        if col in df_map[alias].columns:
                            df_map[alias] = df_map[alias].with_columns(pl.col(col).cast(dtype))

        # Call user transform
        kwargs = {}
        if self.cfg.dt_param_name:
            kwargs[self.cfg.dt_param_name] = part.values.get(self.cfg.dt_param_name)
        out_map: Dict[str, pl.DataFrame] = fn(df_map, **kwargs)

        # Single-output: pick cfg.out_port
        out_df = out_map[self.cfg.out_port]
        return {self.cfg.output: Dataset(rows=out_df.to_dicts())}


def bind_polars_model(cfg: PolarsModelConfig) -> NodeBinding:
    """Build NodeBinding for a single-output Polars transform."""
    # Requires contracts for all inputs (we accept dict row_type; rely on linting to enforce shapes)
    reqs = tuple(ProductContract(product=p, row_type=dict) for p in cfg.inputs.values())
    provs = (ProductContract(product=cfg.output, row_type=dict, required_fields=tuple(cfg.required_output_fields or ())),)
    node = PolarsModuleModelNode(cfg=cfg)
    return NodeBinding(node=node, requires=reqs, provides=provs)
