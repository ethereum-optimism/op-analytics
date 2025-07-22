import polars as pl
from op_analytics.datapipeline.chains.loaders.base import (
    BaseChainMetadataLoader,
    LoaderRegistry,
    log,
)
from op_analytics.datasources.defillama.chaintvl.metadata import ChainsMetadata


class DefiLlamaChainMetadataLoader(BaseChainMetadataLoader):
    """
    Loads chain metadata from DefiLlama API and harmonizes to canonical schema.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def load_data(self, **kwargs) -> pl.DataFrame:
        log.info("Fetching DefiLlama chain metadata via ChainsMetadata.fetch")
        meta = ChainsMetadata.fetch()
        df = meta.df
        # Harmonize to canonical schema
        df = df.with_columns(
            [
                pl.col("chain_id").cast(pl.Int64),
                pl.col("chain_name").cast(pl.String),
                pl.col("chain_name").alias("display_name"),
                pl.lit("defillama").alias("source_name"),
                pl.lit(5).alias("source_rank"),
            ]
        )
        # Only keep required/optional fields
        keep = set(self.REQUIRED_FIELDS + self.OPTIONAL_FIELDS)
        df = df.select([c for c in df.columns if c in keep])
        return df


LoaderRegistry.register("defillama", DefiLlamaChainMetadataLoader)
