import polars as pl
from typing import Dict

from ....core.defs.frame import Frame


def _to_date(col: pl.Expr) -> pl.Expr:
    # handle string ISO dates or already-date columns
    return col.cast(pl.Date, strict=False)


def transform(*, inputs: Dict[str, Frame], dt: str | None = None) -> pl.DataFrame:
    txs  = inputs["txs"];  print("TXS from:", txs.meta.provider, txs.meta.details)
    gas  = inputs["gas"];  print("GAS from:", gas.meta.provider, gas.meta.details)
    fees = inputs["fees"]; print("FEES from:", fees.meta.provider, fees.meta.details)
    txs = inputs["txs"]
    gas = inputs["gas"]
    fees = inputs["fees"]

    # Normalize dates
    txs_df = txs.df.with_columns(
        dt_day=_to_date(pl.col("dt")),
        chain_key=(
            pl.when(pl.col("chain_id").cast(pl.Int64).fill_null(0) != 0)
            .then(pl.col("chain_id").cast(pl.Utf8))
            .otherwise(pl.col("display_name").fill_null(pl.col("blockchain")))
            .str.replace_all(" ", "")
            .str.to_lowercase()
        ),
    )

    if dt:
        txs_df = txs_df.filter(pl.col("dt_day") == pl.lit(dt).str.strptime(pl.Date, strict=False))

    # all_txs (aggregate max per day/chain, require blocks>0)
    all_txs_df = (
        txs_df.group_by(["dt_day", "chain_key", "blockchain"])
           .agg(
               pl.col("num_txs").max().alias("txs_per_day"),
               pl.col("num_blocks").max().alias("blocks_per_day"),
               pl.col("median_tx_fee_usd").max(),
               pl.col("tx_fee_usd").max(),
           )
           .filter(pl.col("blocks_per_day") > 0)
    )

    # all_gas
    gas_df = gas.df.with_columns(
        dt_day=_to_date(pl.col("dt")),
        chain_key=(
            pl.when(pl.col("chain_id").cast(pl.Int64).fill_null(0) != 0)
              .then(pl.col("chain_id").cast(pl.Utf8))
              .otherwise(pl.col("blockchain"))
              .str.replace_all(" ", "")
              .str.to_lowercase()
        ),
    )
    if dt:
        gas_df = gas_df.filter(pl.col("dt_day") == pl.lit(dt).str.strptime(pl.Date, strict=False))
    all_gas_df = (
        gas_df.group_by(["dt_day", "chain_key", "blockchain"])
           .agg(pl.col("sum_evm_gas_used").max())
    )

    # all_fees
    fees_df = fees.df.with_columns(
        dt_day=_to_date(pl.col("dt")),
        chain_key=(
            pl.when(pl.col("chain_id").cast(pl.Int64).fill_null(0) != 0)
              .then(pl.col("chain_id").cast(pl.Utf8))
              .otherwise(pl.col("blockchain"))
              .str.replace_all(" ", "")
              .str.to_lowercase()
        ),
    )
    if dt:
        fees_df = fees_df.filter(pl.col("dt_day") == pl.lit(dt).str.strptime(pl.Date, strict=False))
    all_fees_df = (
        fees_df.group_by(["dt_day", "chain_key", "blockchain"])
            .agg(pl.col("tx_fee_usd").max().alias("sum_tx_fee_usd"))
    )

    j = (
        all_txs_df
        .join(
            all_gas_df.select(["dt_day", "blockchain", "sum_evm_gas_used"]),
            on=["dt_day", "blockchain"],
            how="left",
        )
        .join(
            all_fees_df.select(["dt_day", "blockchain", "sum_tx_fee_usd"]),
            on=["dt_day", "blockchain"],
            how="left",
        )
        .with_columns(
            pl.when(pl.col("blockchain") == "plume")
            .then(pl.lit("98866"))
            .otherwise(pl.col("chain_key"))
            .alias("chain_key_final"),
            (pl.col("sum_evm_gas_used") / (60 * 60 * 24)).alias("sum_evm_gas_used_per_second"),
            pl.coalesce([pl.col("sum_tx_fee_usd"), pl.col("tx_fee_usd")]).alias("tx_fee_usd_final"),
        )
        .group_by(["dt_day", "chain_key_final"])
        .agg(
            pl.col("txs_per_day").max(),
            pl.col("blocks_per_day").max(),
            pl.col("median_tx_fee_usd").max(),
            pl.col("tx_fee_usd_final").max().alias("tx_fee_usd"),
            pl.col("sum_evm_gas_used").max(),
            pl.col("sum_evm_gas_used_per_second").max(),
        )
        .select(
            pl.col("dt_day"),
            pl.col("chain_key_final").alias("chain_key"),
            pl.col("txs_per_day"),
            pl.col("blocks_per_day"),
            pl.col("median_tx_fee_usd"),
            pl.col("tx_fee_usd"),
            pl.col("sum_evm_gas_used"),
            pl.col("sum_evm_gas_used_per_second"),
        )
        .sort(["dt_day", "chain_key"])
    )

    return j
