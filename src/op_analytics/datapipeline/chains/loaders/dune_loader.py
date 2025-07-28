import polars as pl
from op_analytics.datapipeline.chains.loaders.base import BaseChainMetadataLoader, register_loader
from op_analytics.datasources.dune.dextrades import DuneDexTradesSummary


class DuneChainMetadataLoader(BaseChainMetadataLoader):
    def load_data(self) -> pl.DataFrame:
        df = DuneDexTradesSummary.fetch().df
        chain_col = "blockchain" if "blockchain" in df.columns else "chain_name"
        return self.add_metadata_columns(
            df=df, chain_key_col=chain_col, source="dune", source_rank=4
        )


register_loader("dune_loader", DuneChainMetadataLoader)
