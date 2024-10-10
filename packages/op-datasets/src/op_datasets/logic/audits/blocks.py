from dataclasses import dataclass

import polars as pl
from op_coreutils.logger import structlog

log = structlog.get_logger()


@dataclass
class AuditExpr:
    name: str
    expr: pl.Expr | str

    @property
    def col(self) -> pl.Expr:
        if isinstance(self.expr, str):
            _expr = pl.sql_expr(self.expr)
        else:
            _expr = self.expr
        return _expr.alias(self.name)


VALID_HASH = r"^0x[\da-f]{64}$"

BLOCK_AUDITS = [
    #
    # Ensure there are no duplicate block numbers.
    AuditExpr(name="audit_duplicate_blocks", expr="count(*) - count(distinct number)"),
    #
    # Ensure all "hash" values are valid hex strings.
    AuditExpr(name="audit_invalid_hash", expr=(~pl.col("hash").str.contains(VALID_HASH)).sum()),
    #
    # Enxure that block timestamps are increasing.
    AuditExpr(name="audit_timestamps", expr=(~pl.col("hash").str.contains(VALID_HASH)).sum()),
]

TRANSACTION_AUDITS = [
    #
    # Ensure all "hash" values are valid hex strings.
    AuditExpr(name="audit_invalid_hash", expr=(~pl.col("hash").str.contains(VALID_HASH)).sum()),
]


def monotonically_increasing(dataframes: dict[str, pl.DataFrame]):
    diffs = (
        dataframes["blocks"]
        .sort("number")
        .select(
            "number",
            "timestamp",
            (pl.col("number") - pl.col("number").shift(1)).alias("number_diff"),
            (pl.col("timestamp") - pl.col("timestamp").shift(1)).alias("timestamp_diff"),
        )
        .filter(pl.col("number_diff").is_not_null())  # ignore the first row
    )

    result = {
        "block_number": diffs.select((pl.col("number_diff") != 1).sum().alias("audit")),
        "timestamp": diffs.select((pl.col("timestamp_diff") < 0).sum().alias("audit")),
    }

    return result


def distinct_block_numbers(dataframes: dict[str, pl.DataFrame]):
    ctx = pl.SQLContext(frames=dataframes)
    result = ctx.execute(
        """
        SELECT count(*) - count(distinct number) AS audit
        FROM blocks
        """
    ).collect()

    return result
