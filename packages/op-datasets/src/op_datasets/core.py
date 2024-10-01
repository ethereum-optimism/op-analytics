from collections import Counter
from enum import Enum

from pydantic import BaseModel, model_validator
from pyiceberg.types import (
    IcebergType,
)


class JsonRPCMethod(str, Enum):
    eth_getBlockByNumber = 1
    eth_getTransactionReceipt = 2
    eth_getTransactionByHash = 3


class Column(BaseModel):
    # Iceberg Properties
    field_id: int
    name: str
    field_type: IcebergType
    required: bool

    # Custom Properties
    doc: str | None = None
    json_rpc_method: JsonRPCMethod | None = None
    json_rpc_field_name: str | None = None
    enrichment_function: str | None = None

    # Translation Properties

    # The expression used by goldsky on their pipeline product.
    raw_goldsky_pipeline_expr: str | None = None

    # The type used by goldsky on their pipeline product.
    raw_goldsky_pipeline_type: str | None = None

    # The expression used by OP Labs to cast Clickhouse types to the OP Labs type used for this field.
    op_analytics_clickhouse_expr: str | None = None


class Table(BaseModel):
    name: str
    doc: str
    columns: list[Column]

    @model_validator(mode="after")
    def names_are_unique(self) -> "Table":
        def _check_unique(attr):
            values = [getattr(col, attr) for col in self.columns if getattr(col, attr) is not None]
            for key, val in Counter(values).items():
                if val > 1:
                    raise ValueError(f"repeated {attr} name: {key}")

        _check_unique("name")
        _check_unique("op_analytics_clickhouse_expr")

        return self
