from collections import Counter
from enum import Enum

from pydantic import BaseModel, model_validator
from pyiceberg.types import (
    IcebergType,
)
from pyiceberg.types import (
    DoubleType,
    IntegerType,
    ListType,
    LongType,
    StringType,
    TimestampType,
    StructType,
)


class JsonRPCMethod(str, Enum):
    eth_getBlockByNumber = 1
    eth_getTransactionReceipt = 2
    eth_getTransactionByHash = 3


def to_bigquery_type(iceberg_type: IcebergType):
    if iceberg_type == DoubleType():
        return "FLOAT64"

    if iceberg_type == IntegerType() or iceberg_type == LongType():
        return "INT64"

    if iceberg_type == TimestampType():
        return "TIMESTAMP"

    if iceberg_type == StringType():
        return "STRING"

    if isinstance(iceberg_type, ListType):
        element = to_bigquery_type(iceberg_type.element_type)
        return f"ARRAY<{element}>"

    if isinstance(iceberg_type, StructType):
        fields = ", ".join(
            [f"{_.name} {to_bigquery_type(_.field_type)}" for _ in iceberg_type.fields]
        )
        return f"STRUCT<{fields}>"

    raise NotImplementedError()


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

    def display_dict(self):
        oplabs_expr: str

        if self.op_analytics_clickhouse_expr is not None:
            oplabs_expr = (
                self.op_analytics_clickhouse_expr.split("AS")[0].strip()
                if " AS " in self.op_analytics_clickhouse_expr
                else self.op_analytics_clickhouse_expr
            )
        else:
            oplabs_expr = None

        return {
            "Name": self.name,
            "JSON-RPC method": self.json_rpc_method.name if self.json_rpc_method else None,
            "JSON-RPC field": self.json_rpc_field_name,
            "Goldsky Type": self.raw_goldsky_pipeline_type,
            "Goldsky Field": self.raw_goldsky_pipeline_expr,
            "OP Labs BigQuery Type": to_bigquery_type(self.field_type),
            "OP Labs Expression": oplabs_expr,
        }


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
