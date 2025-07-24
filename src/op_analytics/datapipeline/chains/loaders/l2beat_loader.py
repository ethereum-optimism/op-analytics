import polars as pl
from op_analytics.datapipeline.chains.loaders.base import (
    BaseChainMetadataLoader,
    LoaderRegistry,
    log,
)
from op_analytics.datasources.l2beat.projects import L2BeatProjectsSummary
from op_analytics.datapipeline.chains.schemas import harmonize_to_canonical_schema


class L2BeatChainMetadataLoader(BaseChainMetadataLoader):
    """
    Loads chain metadata from L2Beat API and harmonizes to canonical schema.
    """

    def load_data(self, **kwargs) -> pl.DataFrame:
        log.info("Fetching L2Beat chain metadata via L2BeatProjectsSummary.fetch")
        meta = L2BeatProjectsSummary.fetch()
        df = meta.summary_df

        if df.height == 0:
            log.warning("L2Beat API returned no data.")
            return harmonize_to_canonical_schema(pl.DataFrame())

        # Map source fields to canonical names
        df = df.with_columns(
            [
                pl.col("name").alias("chain"),
                pl.coalesce(pl.col("id"), pl.col("name"))
                .str.to_lowercase()
                .str.replace_all(" ", "")
                .alias("chain_key"),
                pl.col("name").alias("display_name"),
                pl.col("category").alias("layer"),
                pl.lit("l2beat").alias("source_name"),
                pl.lit(2).cast(pl.Int32).alias("source_rank"),
                pl.lit(True).alias("is_evm"),
                pl.col("stage").alias("l2b_stage"),
                pl.col("providers").list.first().alias("provider"),
                pl.col("da_badge").alias("da_layer"),
                pl.col("isUpcoming").alias("is_upcoming"),
                pl.col("isArchived").not_().alias("is_current_chain"),
            ]
        )

        # Provider entity harmonization
        df = df.with_columns(
            pl.when(pl.col("provider") == "OP Stack")
            .then(pl.lit("Optimism: OP Stack"))
            .otherwise(None)
            .alias("provider_entity")
        )

        # Harmonize to the canonical schema
        df = harmonize_to_canonical_schema(df)

        log.debug("L2Beat loader output columns", columns=df.columns, shape=df.shape)
        return df


LoaderRegistry.register("l2beat", L2BeatChainMetadataLoader)
