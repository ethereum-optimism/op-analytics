import polars as pl
from .base import BaseChainMetadataLoader, LoaderRegistry, log
from op_analytics.datapipeline.chains.schemas import harmonize_to_canonical_schema


class CsvChainMetadataLoader(BaseChainMetadataLoader):
    """
    Loads chain metadata from a CSV file and harmonizes to the canonical schema.
    Assumes the CSV uses canonical column names where possible.
    """

    def __init__(self, csv_path: str, **kwargs):
        super().__init__(csv_path=csv_path, **kwargs)
        self.csv_path = csv_path

    def load_data(self, **kwargs) -> pl.DataFrame:
        log.info("Loading chain metadata from CSV", path=self.csv_path)
        try:
            df = pl.read_csv(self.csv_path)
        except Exception as e:
            log.error("Failed to load CSV", path=self.csv_path, error=str(e))
            raise

        if "chain" in df.columns and "chain_key" not in df.columns:
            log.debug("Generating 'chain_key' from 'chain' column.")
            df = df.with_columns(
                pl.col("chain").str.to_lowercase().str.replace_all(" ", "-").alias("chain_key")
            )

        # Harmonize to the canonical schema
        df = harmonize_to_canonical_schema(df)

        log.debug("CSV loader output columns", columns=df.columns, shape=df.shape)
        return df


LoaderRegistry.register("csv", CsvChainMetadataLoader)
