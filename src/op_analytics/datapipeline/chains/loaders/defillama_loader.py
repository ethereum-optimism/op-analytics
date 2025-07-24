import polars as pl
from op_analytics.datapipeline.chains.loaders.base import (
    BaseChainMetadataLoader,
    LoaderRegistry,
    log,
)
from op_analytics.datasources.defillama.chaintvl.metadata import ChainsMetadata
from op_analytics.datapipeline.chains.schemas import harmonize_to_canonical_schema


class DefiLlamaChainMetadataLoader(BaseChainMetadataLoader):
    """
    Loads chain metadata from DefiLlama API and harmonizes to canonical schema.
    """

    def load_data(self, **kwargs) -> pl.DataFrame:
        log.info("Fetching DefiLlama chain metadata via ChainsMetadata.fetch")
        meta = ChainsMetadata.fetch()
        df = meta.df

        if df.height == 0:
            log.warning("DefiLlama API returned no data.")
            return harmonize_to_canonical_schema(pl.DataFrame())

        # Rename columns for clarity
        df = df.rename({"chain_name": "chain", "gecko_id": "gas_token"})

        # Map source fields to canonical names
        df = df.with_columns(
            [
                pl.col("chain").alias("display_name"),
                pl.coalesce(pl.col("gas_token"), pl.col("chain"))
                .str.to_lowercase()
                .str.replace_all(" ", "-")
                .alias("chain_key"),
                pl.col("chain_id").cast(pl.Utf8),
                pl.lit("defillama").alias("source_name"),
                pl.lit(5).cast(pl.Int32).alias("source_rank"),
            ]
        )

        # Simple layer harmonization
        df = df.with_columns(
            pl.when(pl.col("is_superchain") == 1)
            .then(pl.lit("L2"))
            .otherwise(pl.col("layer"))
            .alias("layer")
        )

        # Provider harmonization
        df = df.with_columns(
            pl.when(pl.col("chain").str.contains("OP Stack"))
            .then(pl.lit("OP Stack"))
            .otherwise(None)
            .alias("provider"),
            pl.when(pl.col("chain").str.contains("OP Stack"))
            .then(pl.lit("Optimism: OP Stack"))
            .otherwise(None)
            .alias("provider_entity"),
        )

        # Boolean flags
        df = df.with_columns(
            pl.lit(True).alias("is_current_chain"),
            pl.lit(False).alias("is_upcoming"),
        )

        # Harmonize to the canonical schema
        df = harmonize_to_canonical_schema(df)

        log.debug("DefiLlama loader output columns", columns=df.columns, shape=df.shape)
        return df


LoaderRegistry.register("defillama", DefiLlamaChainMetadataLoader)
