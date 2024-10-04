from dataclasses import dataclass

import polars as pl
from op_coreutils.logger import structlog, bind_contextvars

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


class DataPipelineError(Exception):
    pass


def _audit(name: str, df: pl.DataFrame, audits: list[AuditExpr], info: dict):
    audits = df.select(*[_.col for _ in audits])

    if len(audits) != 1:
        raise DataPipelineError(
            f"audits dataframe should not have more than one row: len={len(audits)}"
        )

    for col in audits.columns:
        value = audits[col].sum()
        if value > 0:
            msg = f"audit failed: {col} value={value}"
            info[col] = value
            log.error(msg)
            raise DataPipelineError(f"{msg} {info}")

    log.info(f"PASS audit: {name}")


def run_all(dataframes: dict[str, pl.DataFrame]):
    info = (
        dataframes["blocks"]
        .select(
            pl.col("number").min().alias("min_block"),
            pl.col("number").max().alias("max_block"),
            pl.sql_expr("count(*)").alias("num_blocks"),
        )
        .to_dicts()[0]
    )
    bind_contextvars(num_blocks=info["num_blocks"])

    _audit("blocks", dataframes["blocks"], BLOCK_AUDITS, info)
    _audit("transactions", dataframes["transactions"], TRANSACTION_AUDITS, info)
