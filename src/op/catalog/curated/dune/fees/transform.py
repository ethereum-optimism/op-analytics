from typing import Mapping, Optional, Dict, Any
import polars as pl

from op.catalog.config.chain_overrides import CHAIN_OVERRIDES, normalize_chain_key

def _safe_utf8(col: pl.Expr) -> pl.Expr:
    return pl.when(col.is_null()).then(pl.lit(None)).otherwise(col.cast(pl.Utf8))

def _safe_int(col: pl.Expr) -> pl.Expr:
    return pl.when(col.is_null()).then(pl.lit(None)).otherwise(col.cast(pl.Int64))

def _safe_f64(col: pl.Expr) -> pl.Expr:
    return pl.when(col.is_null()).then(pl.lit(None)).otherwise(col.cast(pl.Float64))

def transform(
    frames: Mapping[str, pl.DataFrame],
    dt: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
) -> pl.DataFrame:
    """
    Input: frames['txs'] with columns similar to:
      dt, blockchain, num_txs, num_blocks, median_tx_fee_usd, tx_fee_usd, chain_id?, display_name?
    Output: standardized frame per schema.Record (above).
    """
    txs = frames["txs"]

    # normalize types
    txs = txs.with_columns(
        _safe_utf8(pl.col("blockchain")).alias("blockchain"),
        _safe_utf8(pl.col("display_name")).alias("display_name"),
        _safe_int(pl.col("chain_id")).alias("chain_id"),
        _safe_int(pl.col("num_txs")).alias("num_txs"),
        _safe_int(pl.col("num_blocks")).alias("num_blocks"),
        _safe_f64(pl.col("median_tx_fee_usd")).alias("median_tx_fee_usd"),
        _safe_f64(pl.col("tx_fee_usd")).alias("tx_fee_usd"),
        _safe_utf8(pl.col("dt")).alias("dt"),
    )

    # optional day filter
    if dt:
        txs = txs.filter(pl.col("dt") == pl.lit(dt))

    # apply overrides
    # materialize override columns then compute chain_key
    def override_display_name(expr_bc: pl.Expr, expr_dn: pl.Expr) -> pl.Expr:
        return pl.when(
            pl.col("__ovr_display").is_not_null()
        ).then(pl.col("__ovr_display")).otherwise(expr_dn)

    def override_chain_id(expr_cid: pl.Expr) -> pl.Expr:
        return pl.when(
            pl.col("__ovr_chain_id").is_not_null()
        ).then(pl.col("__ovr_chain_id")).otherwise(expr_cid)

    # build override lookup as structs→DataFrame join
    # Map: blockchain_lower -> {ovr_display, ovr_chain_id}
    ovr_rows = []
    for bc, ov in CHAIN_OVERRIDES.items():
        ovr_rows.append({"__bc": bc, "__ovr_display": ov.display_name, "__ovr_chain_id": ov.chain_id})
    ovr = pl.DataFrame(ovr_rows) if ovr_rows else pl.DataFrame(schema={"__bc": pl.Utf8, "__ovr_display": pl.Utf8, "__ovr_chain_id": pl.Int64})

    txs = txs.with_columns(pl.col("blockchain").str.to_lowercase().alias("__bc"))
    txs = txs.join(ovr, on="__bc", how="left")

    txs = txs.with_columns(
        override_display_name(pl.col("blockchain"), pl.col("display_name")).alias("display_name"),
        override_chain_id(pl.col("chain_id")).alias("chain_id"),
    )

    # compute chain_key
    # We can't call Python per-row easily, so reconstruct via expressions:
    txs = txs.with_columns(
        pl.when(pl.col("chain_id").is_not_null() & (pl.col("chain_id") > 0))
        .then(pl.col("chain_id").cast(pl.Utf8))
        .otherwise(
            pl.when(pl.col("display_name").is_not_null())
            .then(pl.col("display_name"))
            .otherwise(pl.col("blockchain"))
        )
        .str.replace_all(" ", "")
        .str.to_lowercase()
        .alias("chain_key")
    )

    # final shape
    out = txs.select(
        pl.col("dt").str.strptime(pl.Date, strict=False).alias("dt_day"),
        "chain_key",
        "blockchain",
        "display_name",
        "chain_id",
        pl.col("num_txs").alias("txs_per_day"),
        pl.col("num_blocks").alias("blocks_per_day"),
        "median_tx_fee_usd",
        "tx_fee_usd",
    )

    return out
