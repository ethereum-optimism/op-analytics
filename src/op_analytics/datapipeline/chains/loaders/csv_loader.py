import polars as pl
from .base import BaseChainMetadataLoader, LoaderRegistry, log


class CsvChainMetadataLoader(BaseChainMetadataLoader):
    """
    Loads chain metadata from a CSV file.
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
        return df

    def preprocess(self, df: pl.DataFrame) -> pl.DataFrame:
        for col in df.columns:
            if df.schema[col] == pl.String:
                df = df.with_columns(df[col].str.strip_chars().alias(col))
        return df


LoaderRegistry.register("csv", CsvChainMetadataLoader)
