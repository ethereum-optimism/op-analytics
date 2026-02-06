import re
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

    raise NotImplementedError(f"Unsupported Arrow type: {arrow_type}")


# -------------------------------------------------------------------
# ClickHouse -> Arrow safety for wide ints (UInt128/Int128/UInt256/Int256)
# -------------------------------------------------------------------

# Minimal blast radius: always protect these column names even if type metadata is missing.
_FORCE_SAFE_INT_CAST_COLUMNS: set[str] = {
    "receipt_l1_blob_base_fee",
}

# Find Int/UInt widths inside strings like "UInt128" or "Nullable(UInt128)".
_CH_INT_RE = re.compile(r"\b(U?Int)(\d+)\b")


def _infer_clickhouse_int_width(type_str: str | None) -> tuple[bool, int] | None:
    """
    Returns (is_unsigned, bits) if type_str contains Int*/UInt*.
    Examples:
      "UInt128" -> (True, 128)
      "Nullable(UInt128)" -> (True, 128)
      "Int64" -> (False, 64)
    """
    if not type_str:
        return None
    m = _CH_INT_RE.search(type_str)
    if not m:
        return None
    is_unsigned = m.group(1) == "UInt"
    bits = int(m.group(2))
    return is_unsigned, bits


def _split_alias(expr: str) -> tuple[str, str | None]:
    """
    Split "<lhs> AS <alias>" using the LAST " AS " occurrence.
    """
    s = expr.strip()
    if " AS " in s:
        lhs, alias = s.rsplit(" AS ", 1)
        return lhs.strip(), alias.strip()
    return s, None


def _needs_safe_int_cast(col: "Column") -> bool:
    """
    Apply accurateCastOrNull to prevent Arrow crash when ClickHouse has to serialize wide ints.

    Rule:
      - Only for target ints (pa.int32/pa.int64)
      - If raw ClickHouse type width > target width (e.g. UInt128 -> Int64)
      - OR if the column is force-listed
    """
    if col.field_type not in (pa.int32(), pa.int64()):
        return False

    if col.name in _FORCE_SAFE_INT_CAST_COLUMNS:
        return True

    raw = _infer_clickhouse_int_width(col.raw_goldsky_pipeline_type)
    if raw is None:
        return False

    _is_unsigned, raw_bits = raw
    target_bits = 32 if col.field_type == pa.int32() else 64
    return raw_bits > target_bits


def _safe_cast_expr(col: "Column") -> str:
    """
    Produce ClickHouse expression:
      accurateCastOrNull(<raw_expr>, 'Int64') AS <name>

    Uses raw_goldsky_pipeline_expr (preferred), else falls back to the column name.
    """
    base = (col.raw_goldsky_pipeline_expr or col.name).strip()
    target = "Int32" if col.field_type == pa.int32() else "Int64"
    # accurateCastOrNull always returns Nullable(T) and returns NULL if not representable. :contentReference[oaicite:1]{index=1}
    return f"accurateCastOrNull({base}, '{target}') AS {col.name}"


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

    def goldsky_sql(self, source_table: str, where: str | None = None):
        """Query to read the source table from the Goldsky Clickhouse instance.

        The clickhouse expr is used for each column in the dataset to formulate the
        SQL query. The optional "where" filter is typically used to narrow down the
        query to a specific range of block numbers.

        NEW behavior:
          - If a column is INT32/INT64 in our dataset but raw type is wider (UInt128, etc),
            generate: accurateCastOrNull(raw_expr, 'Int64') AS <col>
            so overflow becomes NULL and Arrow serialization cannot crash.
        """
        exprs: list[str] = []

        for col in self.columns:
            if col.op_analytics_clickhouse_expr is None:
                continue

            # If we need safe casting, override the expression entirely for this column.
            if _needs_safe_int_cast(col):
                exprs.append("    " + _safe_cast_expr(col))
                continue

            exprs.append("    " + col.op_analytics_clickhouse_expr)

        cols = ",\n".join(exprs)

        if where is None:
            return f"SELECT\n{cols}\nFROM {source_table}"
        else:
            return f"SELECT\n{cols}\nFROM {source_table} WHERE {where}"
