import polars as pl
from op_analytics.datapipeline.chains.loaders.base import (
    BaseChainMetadataLoader,
    LoaderRegistry,
    log,
)
from op_analytics.datasources.dune.dextrades import DuneDexTradesSummary
from op_analytics.datapipeline.chains.schemas import harmonize_to_canonical_schema


class DuneChainMetadataLoader(BaseChainMetadataLoader):
    """
    Loads chain metadata from Dune DEX trades summary and harmonizes to canonical schema.
    Uses 'blockchain' column if 'chain' is missing.
    """

    def load_data(self, **kwargs) -> pl.DataFrame:
        log.info("Fetching Dune chain metadata via DuneDexTradesSummary.fetch")
        meta = DuneDexTradesSummary.fetch()
        df = meta.df

        if df.height == 0:
            log.warning("Dune API returned no data.")
            return harmonize_to_canonical_schema(pl.DataFrame())

        chain_col = "blockchain" if "blockchain" in df.columns else "chain"

        chain_key_expr = pl.col(chain_col)
        if "chain_id" in df.columns:
            chain_key_expr = pl.coalesce(pl.col("chain_id").cast(pl.Utf8), chain_key_expr)

        is_evm_expr = pl.lit(True)
        if "chain_id" in df.columns:
            is_evm_expr = (
                pl.when(pl.col("chain_id").is_not_null())
                .then(pl.lit(True))
                .otherwise(pl.lit(False))
            )

        df = (
            df.with_columns(
                [
                    pl.col(chain_col).alias("chain"),
                    chain_key_expr.str.to_lowercase().str.replace_all(" ", "").alias("chain_key"),
                    pl.col(chain_col).alias("display_name"),
                    pl.lit("dune").alias("source_name"),
                    pl.lit(4).cast(pl.Int32).alias("source_rank"),
                    is_evm_expr.alias("is_evm"),
                    pl.col("dt").alias("min_dt_day"),
                    pl.col("dt").alias("max_dt_day"),
                ]
            )
            .group_by("chain_key")
            .agg(
                pl.first("chain"),
                pl.first("display_name"),
                pl.first("source_name"),
                pl.first("source_rank"),
                pl.first("is_evm"),
                pl.min("min_dt_day"),
                pl.max("max_dt_day"),
            )
        )

        # Harmonize to the canonical schema
        df = harmonize_to_canonical_schema(df)

        log.debug("Dune loader output columns", columns=df.columns, shape=df.shape)
        return df


LoaderRegistry.register("dune", DuneChainMetadataLoader)
