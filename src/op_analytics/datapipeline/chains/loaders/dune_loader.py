import polars as pl
from op_analytics.datapipeline.chains.loaders.base import (
    BaseChainMetadataLoader,
    LoaderRegistry,
    log,
)
from op_analytics.datasources.dune.dextrades import DuneDexTradesSummary


class DuneChainMetadataLoader(BaseChainMetadataLoader):
    """
    Loads chain metadata from Dune DEX trades summary and harmonizes to canonical schema.
    Uses 'blockchain' column if 'chain' is missing.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def load_data(self, **kwargs) -> pl.DataFrame:
        log.info("Fetching Dune chain metadata via DuneDexTradesSummary.fetch")
        meta = DuneDexTradesSummary.fetch()
        df = meta.df
        # Use 'chain' if present, else 'blockchain', else error
        chain_col = None
        if "chain" in df.columns:
            chain_col = "chain"
        elif "blockchain" in df.columns:
            chain_col = "blockchain"
        else:
            raise ValueError(
                "Dune DEX trades summary must have a 'chain' or 'blockchain' column for harmonization."
            )
        df = df.with_columns(
            [
                pl.col(chain_col).cast(pl.String).alias("chain_name"),
                pl.col(chain_col).alias("display_name"),
                pl.lit(None).cast(pl.Int64).alias("chain_id"),
                pl.lit("dune").alias("source_name"),
                pl.lit(4.5).alias("source_rank"),
            ]
        )
        keep = set(self.REQUIRED_FIELDS + self.OPTIONAL_FIELDS)
        df = df.select([c for c in df.columns if c in keep])
        return df


LoaderRegistry.register("dune", DuneChainMetadataLoader)
