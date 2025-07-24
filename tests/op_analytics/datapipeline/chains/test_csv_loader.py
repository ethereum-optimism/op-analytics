import polars as pl
from op_analytics.datapipeline.chains.loaders.csv_loader import CsvChainMetadataLoader
import tempfile
from op_analytics.datapipeline.chains.schemas import CHAIN_METADATA_SCHEMA

# Mock DataFrame with all required columns as expected by the loader
SAMPLE_DF = pl.DataFrame(
    {
        "chain_id": [1, 2],
        "chain_name": ["TestChain", "AnotherChain"],
        "display_name": ["Test Chain", "Another Chain"],
        "source_name": ["example_source", "example_source"],
        "source_rank": [1, 2],
        "dt_day": ["2024-01-01", "2024-01-02 "],  # Note the trailing space for test
    }
)

SAMPLE_CSV = tempfile.NamedTemporaryFile(delete=False, mode="w", suffix=".csv")
SAMPLE_DF.write_csv(SAMPLE_CSV.name)
SAMPLE_CSV.close()


def test_csv_loader_basic(tmp_path):
    # Create a dummy CSV file
    csv_content = """chain,display_name,chain_id
optimism,Optimism,10
base,Base,8453
"""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text(csv_content)

    # Run the loader
    loader = CsvChainMetadataLoader(csv_path=str(csv_file))
    df = loader.run()

    # Assertions
    assert isinstance(df, pl.DataFrame)
    assert df.height == 2
    assert "chain_key" in df.columns
    assert df["chain_key"][0] == "optimism"

    # Check that it's harmonized
    assert list(df.columns) == list(CHAIN_METADATA_SCHEMA.keys())


def test_csv_loader_empty(tmp_path):
    # Create an empty CSV file with headers
    csv_content = "chain,display_name,chain_id\\n"
    csv_file = tmp_path / "test.csv"
    csv_file.write_text(csv_content)

    loader = CsvChainMetadataLoader(csv_path=str(csv_file))
    df = loader.run()

    assert df.height == 0
    assert list(df.columns) == list(CHAIN_METADATA_SCHEMA.keys())
