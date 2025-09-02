from dataclasses import dataclass, fields, is_dataclass
from typing import Type

from ..types.dataset import Dataset
from ..types.product_ref import ProductRef
from ..utils.helpers import field_names


@dataclass(frozen=True)
class ProductContract:
    product: ProductRef
    row_type: Type
    required_fields: tuple[str, ...] = ()

    def validate_against(self, ds: Dataset) -> None:
        if is_dataclass(self.row_type) and ds.rows:
            # quick structural check on first row if it's a dict
            if isinstance(ds.rows[0], dict):
                missing = set(self.required_fields) - set(ds.rows[0].keys())
                if missing:
                    raise AssertionError(f"{self.product.name}: missing fields {missing} in dataset")
        # compile-time-ish check: declared row_type exposes these fields
        fset = field_names(self.row_type)
        missing_decl = set(self.required_fields) - fset if fset else set()
        if missing_decl:
            raise AssertionError(f"{self.product.name}: declared row_type lacks fields {missing_decl}")
