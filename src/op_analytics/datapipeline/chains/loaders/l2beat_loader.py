import polars as pl
from op_analytics.datapipeline.chains.loaders.base import BaseChainMetadataLoader, register_loader
from op_analytics.datasources.l2beat.projects import L2BeatProjectsSummary


class L2BeatChainMetadataLoader(BaseChainMetadataLoader):
    def load_data(self) -> pl.DataFrame:
        df = L2BeatProjectsSummary.fetch().summary_df
        return self.add_metadata_columns(df=df, chain_key_col="id", source="l2beat", source_rank=2)


register_loader("l2beat_loader", L2BeatChainMetadataLoader)
