import polars as pl
from op_analytics.datapipeline.chains.loaders.base import (
    BaseChainMetadataLoader,
    LoaderRegistry,
    log,
)
from op_analytics.datasources.l2beat.projects import L2BeatProjectsSummary


class L2BeatChainMetadataLoader(BaseChainMetadataLoader):
    """
    Loads chain metadata from L2Beat API and harmonizes to canonical schema.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def load_data(self, **kwargs) -> pl.DataFrame:
        log.info("Fetching L2Beat chain metadata via L2BeatProjectsSummary.fetch")
        meta = L2BeatProjectsSummary.fetch()
        df = meta.summary_df
        # Harmonize to canonical schema
        df = df.with_columns(
            [
                pl.col("id").cast(pl.String).alias("chain_id"),
                pl.col("name").cast(pl.String).alias("chain_name"),
                pl.col("name").alias("display_name"),
                pl.lit("l2beat").alias("source_name"),
                pl.lit(2).alias("source_rank"),
            ]
        )
        keep = set(self.REQUIRED_FIELDS + self.OPTIONAL_FIELDS)
        df = df.select([c for c in df.columns if c in keep])
        return df


LoaderRegistry.register("l2beat", L2BeatChainMetadataLoader)
