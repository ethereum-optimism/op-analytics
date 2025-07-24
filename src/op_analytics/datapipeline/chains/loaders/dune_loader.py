import polars as pl
from op_analytics.coreutils.logger import structlog
from op_analytics.datapipeline.chains.loaders.base import BaseChainMetadataLoader, LoaderRegistry
from op_analytics.datapipeline.chains.schemas import (
    harmonize_to_canonical_schema,
    generate_chain_key,
)
from op_analytics.datasources.dune.dextrades import DuneDexTradesSummary

log = structlog.get_logger()


class DuneChainMetadataLoader(BaseChainMetadataLoader):
    def load_data(self, **kwargs) -> pl.DataFrame:
        log.info("Loading Dune chain metadata")
        meta = DuneDexTradesSummary.fetch()
        df = meta.df

        if df.height == 0:
            log.warning("Dune returned no data.")
            return harmonize_to_canonical_schema(pl.DataFrame())

        # Use blockchain column for chain_key generation
        chain_col = "blockchain" if "blockchain" in df.columns else "chain_name"

        df = df.with_columns(
            [
                generate_chain_key(chain_col),
                pl.lit("dune").alias("source"),
                pl.lit(4).alias("source_rank"),
            ]
        )

        df = harmonize_to_canonical_schema(df)
        log.info(f"Loaded {len(df)} chains from Dune")
        return df


LoaderRegistry.register("dune_loader", DuneChainMetadataLoader)
