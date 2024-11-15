from dataclasses import dataclass
from collections import Counter
from enum import Enum

import pyarrow as pa


class JsonRPCMethod(str, Enum):
    eth_getBlockByNumber = 1
    eth_getTransactionReceipt = 2
    eth_getTransactionByHash = 3
    eth_getLogs = 4


def to_bigquery_type(arrow_type: pa.DataType) -> str:
    if arrow_type == pa.float64():
        return "FLOAT64"

    if arrow_type == pa.int32() or arrow_type == pa.int64():
        return "INT64"

    if arrow_type == pa.timestamp(unit="us"):
        return "TIMESTAMP"

    if arrow_type == pa.large_string():
        return "STRING"

    if pa.types.is_large_list(arrow_type):
        element = to_bigquery_type(arrow_type.value_type)
        return f"ARRAY<{element}>"

    if pa.types.is_struct(arrow_type):
        fields = []
        for i in range(arrow_type.num_fields):
            field = arrow_type[i]
            fields.append(f"{field.name} {to_bigquery_type(field.type)}")

        fields_str = ", ".join(fields)
        return f"STRUCT<{fields_str}>"

    raise NotImplementedError()


@dataclass
class Column:
    # Iceberg Properties
    name: str
    field_type: pa.DataType
    required: bool

    # Custom Properties
    doc: str | None = None
    json_rpc_method: JsonRPCMethod | None = None
    json_rpc_field_name: str | None = None

    # Translation Properties

    # The expression used by goldsky on their pipeline product.
    raw_goldsky_pipeline_expr: str | None = None

    # The type used by goldsky on their pipeline product.
    raw_goldsky_pipeline_type: str | None = None

    # The expression used by OP Labs to cast Clickhouse types to the OP Labs type used for this field.
    op_analytics_clickhouse_expr: str | None = None

    # The name of the function used by OP Labs to derive the value from the raw fields.
    op_analytics_enrichment_function: str | None = None

    def display_dict(self) -> dict[str, str | None]:
        oplabs_expr: str | None

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


@dataclass
class CoreDataset:
    name: str
    versioned_location: str
    goldsky_table_suffix: str
    block_number_col: str
    doc: str
    columns: list[Column]

    def __post_init__(self):
        """Run sanity checks."""

        def _check_unique(attr):
            values = [getattr(col, attr) for col in self.columns if getattr(col, attr) is not None]
            for key, val in Counter(values).items():
                if val > 1:
                    raise ValueError(f"repeated {attr} name: {key}")

        _check_unique("name")
        _check_unique("op_analytics_clickhouse_expr")

    def goldsky_sql(
        self,
        source_table: str,
        where: str | None = None,
    ):
        """Query to read the source table from the Goldsky Clickhouse instance.

        The clickhouse expr is used for each column in the dataset to formulate the
        SQL query. The optional "where" filter is typically used to narrow down the
        query to a specific range of block numbers.
        """
        exprs = [
            "    " + _.op_analytics_clickhouse_expr
            for _ in self.columns
            if _.op_analytics_clickhouse_expr is not None
        ]
        cols = ",\n".join(exprs)

        if where is None:
            return f"SELECT\n{cols}\nFROM {source_table}"
        else:
            return f"SELECT\n{cols}\nFROM {source_table} WHERE {where}"
